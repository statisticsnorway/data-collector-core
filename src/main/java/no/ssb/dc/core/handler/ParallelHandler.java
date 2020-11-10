package no.ssb.dc.core.handler;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.Termination;
import no.ssb.dc.api.TerminationException;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.BodyHandler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Execute;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.Parallel;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.executor.FixedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Handler(forClass = Parallel.class)
public class ParallelHandler extends AbstractNodeHandler<Parallel> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelHandler.class);
    public static final String MAX_NUMBER_OF_ITERATIONS = "MAX_NUMBER_OF_ITERATIONS";
    static final String ADD_BODY_CONTENT = "ADD_BODY_CONTENT";
    public static final AtomicLong countNumberOfIterations = new AtomicLong(-1);
    private static final AtomicLong pageCounter = new AtomicLong(-1);

    public ParallelHandler(Parallel node) {
        super(node);
    }

    private PageContext buildPageContext(ExecutionContext input) {
        PageContext.Builder pageContextBuilder = input.state(PageContext.Builder.class);
        input.releaseState(PageContext.Builder.class);
        return pageContextBuilder.build();
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        Response response = input.state(Response.class);

        if (input.state(MAX_NUMBER_OF_ITERATIONS) != null) {
            countNumberOfIterations.incrementAndGet();
        }

        // add correlation-id before fan-out
        //CorrelationIds.of(input).add();

        FixedThreadPool threadPool = input.services().get(FixedThreadPool.class);

        PageContext pageContext = buildPageContext(input);

        DocumentParserFeature parser = Queries.parserFor(node.splitQuery().getClass());

        ConfigurationMap configurationMap = input.services().get(ConfigurationMap.class);
        int queueBufferThreshold = configurationMap.contains("data.collector.parallel.queueBuffer.threshold") ?
                Integer.parseInt(configurationMap.get("data.collector.parallel.queueBuffer.threshold")) : -1;
        LOG.info("Parallel Queue Buffer Threshold: {}", queueBufferThreshold);
        if (queueBufferThreshold > -1) pageCounter.incrementAndGet();

        if (response.bodyHandler().isPresent()) {
            // split using sequential token deserializer
            //doTokenizerQuery(input, response, positionList);
            List<CompletableFuture<ExecutionContext>> parallelFutures = new ArrayList<>();
            AtomicLong counter = new AtomicLong();
            AtomicBoolean cancelLoop = new AtomicBoolean();
            doTokenizerQuery(response, pageEntryDocument -> {
                Consumer<Boolean> cancelCallback = test -> cancelLoop.compareAndSet(false, test);
                CompletableFuture<ExecutionContext> parallelFuture = CompletableFuture
                        .supplyAsync(handlePageEntryDocument(input, pageContext, parser, threadPool, pageEntryDocument, cancelCallback)::join, threadPool.getExecutor());

                if (cancelLoop.get()) {
                    return;
                }

                parallelFutures.add(parallelFuture);

                if (counter.incrementAndGet() % 1000 == 0) {
                    LOG.warn("Wait for buffered futures to complete");
                    CompletableFuture.allOf(parallelFutures.toArray(new CompletableFuture[0]))
                            .thenApply(v -> {
                                parallelFutures.clear();
                                return v;
                            })
                            .join();
                }

                if (counter.get() % 50000 == 0) {
                    LOG.trace("Parallel count: {}", counter.get());
                }
            });
            LOG.info("Parallel count: {}", counter.get());

            LOG.warn("Wait for ALL buffered futures to complete");
            CompletableFuture.allOf(parallelFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        parallelFutures.clear();
                        return v;
                    })
                    .join();

        } else if (queueBufferThreshold > -1) {
            List<CompletableFuture<ExecutionContext>> parallelFutures = new ArrayList<>();
            AtomicLong counter = new AtomicLong();
            AtomicBoolean cancelLoop = new AtomicBoolean();

            List<?> pageList = Queries.from(input, node.splitQuery()).evaluateList(response.body());
            for (Object pageEntryDocument : pageList) {
                Consumer<Boolean> cancelCallback = test -> cancelLoop.compareAndSet(false, test);
                CompletableFuture<ExecutionContext> parallelFuture = CompletableFuture
                        .supplyAsync(handlePageEntryDocument(input, pageContext, parser, threadPool, pageEntryDocument, cancelCallback)::join, threadPool.getExecutor());

                if (cancelLoop.get()) {
                    break;
                }

                parallelFutures.add(parallelFuture);

                if (counter.incrementAndGet() % queueBufferThreshold == 0) {
                    LOG.warn("Wait for buffered futures to complete at: {} / {}", pageCounter.get(), counter.get());
                    CompletableFuture.allOf(parallelFutures.toArray(new CompletableFuture[0]))
                            .thenApply(v -> {
                                parallelFutures.clear();
                                return v;
                            })
                            .join();
                }

                if (counter.get() % 50000 == 0) {
                    LOG.trace("Parallel count: {}", counter.get());
                }
            }
            LOG.info("Parallel count: {}", counter.get());

            // return remaining incomplete futures to PaginationLifecycle
            parallelFutures.forEach(pageContext::addFuture);

        } else {
            // split by query
            List<?> pageList = Queries.from(input, node.splitQuery()).evaluateList(response.body());
            for (Object pageEntryDocument : pageList) {
                CompletableFuture<ExecutionContext> parallelFuture = handlePageEntryDocument(input, pageContext, parser, threadPool, pageEntryDocument, test -> {
                });
                if (parallelFuture == null) {
                    break;
                }
                pageContext.addFuture(parallelFuture);
            }
        }

        //CorrelationIds correlationIds = CorrelationIds.of(input);
        return ExecutionContext.empty().state(PageContext.class, pageContext); // .merge(correlationIds.context())
    }

    // return null will break callee loop
    CompletableFuture<ExecutionContext> handlePageEntryDocument(ExecutionContext input,
                                                                PageContext pageContext,
                                                                DocumentParserFeature parser,
                                                                FixedThreadPool threadPool,
                                                                Object pageEntryDocument,
                                                                Consumer<Boolean> cancelCallback) {

        if (input.state(MAX_NUMBER_OF_ITERATIONS) != null) {
            long maxNumberOfIterations = Long.parseLong(input.state(MAX_NUMBER_OF_ITERATIONS).toString());
            if (ParallelHandler.countNumberOfIterations.get() >= maxNumberOfIterations) {
                pageContext.setEndOfStream(true);
                cancelCallback.accept(true);
                return CompletableFuture.completedFuture(ExecutionContext.empty());
            }
        }

        byte[] serializedItem = parser.serialize(pageEntryDocument);

        /*
         * Resolve variables
         */
        node.variableNames().forEach(variableKey -> {
            try {
                String value = Queries.from(input, node.variable(variableKey)).evaluateStringLiteral(pageEntryDocument);
                input.variable(variableKey, value);
            } catch (RuntimeException | Error e) {
                LOG.error("Error evaluating variable: {} => {} in document: {}", variableKey, node.variable(variableKey), new String(serializedItem, StandardCharsets.UTF_8));
                throw e;
            } catch (Exception e) {
                LOG.error("Error evaluating variable: {} => {} in document: {}", variableKey, node.variable(variableKey), new String(serializedItem, StandardCharsets.UTF_8));
                throw new ParallelException(e);
            }
        });

        /*
         * execute step nodes
         */

        ExecutionContext nodeInput = ExecutionContext.of(input);
        CompletableFuture<ExecutionContext> parallelFuture = createCompletableFuture(input, nodeInput, pageContext, threadPool, pageEntryDocument, serializedItem);

        if (ParallelHandler.countNumberOfIterations.get() > -1) {
            ParallelHandler.countNumberOfIterations.incrementAndGet();
        }

        cancelCallback.accept(false);
        return parallelFuture;
    }

    CompletableFuture<ExecutionContext> createCompletableFuture(ExecutionContext input,
                                                                ExecutionContext nodeInput,
                                                                PageContext pageContext,
                                                                FixedThreadPool threadPool,
                                                                Object pageEntryDocument,
                                                                byte[] serializedItem) {

        CompletableFuture<ExecutionContext> parallelFuture = CompletableFuture
                .supplyAsync(() -> {
                    if (pageContext.isFailure()) {
                        pageContext.setEndOfStream(true);
                        return ExecutionContext.empty();
                    }

                    checkTerminationSignal(input.services().get(Termination.class));

                    ExecutionContext accumulated = ExecutionContext.empty();

                    for (Node step : node.steps()) {
                        ExecutionContext stepInput = ExecutionContext.of(nodeInput).merge(accumulated);
                        stepInput.state(PageEntryState.class, new PageEntryState(pageEntryDocument, serializedItem));

                        // set state nestedOperation to true to inform AddContentHandler to buffer response body
                        if (step instanceof Execute) {
                            stepInput.state(ADD_BODY_CONTENT, true);
                        }

                        ExecutionContext stepOutput = Executor.execute(step, stepInput);

                        checkTerminationSignal(input.services().get(Termination.class));

                        accumulated.merge(stepOutput);
                    }

                    pageContext.incrementQueueCount();

                    return accumulated;

                }, threadPool.getExecutor())
                .thenApply(stepOutput -> {
                    if (pageContext.isFailure()) {
                        pageContext.setEndOfStream(true);
                        return stepOutput;
                    }

                    checkTerminationSignal(input.services().get(Termination.class));

//                        if (true) throw new RuntimeException("blow");

                    pageContext.incrementCompletionCount();

                    return stepOutput;

                }).exceptionally(throwable -> {
                    if (!pageContext.failureCause().compareAndSet(null, throwable)) {
                        //LOG.error("Unable to store throwable in failedException, already set. Current exception: {}", CommonUtils.captureStackTrace(throwable));
                    }
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }
                    throw new ParallelException(throwable);
                });

        return parallelFuture;
    }

    void doTokenizerQuery(Response response, Consumer<Object> entry) {
        try {
            BodyHandler<Path> fileBodyHandler = response.<Path>bodyHandler().orElseThrow();
            DocumentParserFeature parser = Queries.parserFor(node.splitQuery().getClass());

            try (FileInputStream fis = new FileInputStream(fileBodyHandler.body().toFile())) {
                parser.tokenDeserializer(fis, entry);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkTerminationSignal(Termination termination) {
        if (termination != null && termination.isTerminated()) {
            throw new TerminationException();
        }
    }

}
