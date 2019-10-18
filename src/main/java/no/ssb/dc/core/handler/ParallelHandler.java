package no.ssb.dc.core.handler;

import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Execute;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.Parallel;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.executor.FixedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Handler(forClass = Parallel.class)
public class ParallelHandler extends AbstractNodeHandler<Parallel> {

    public static final String MAX_NUMBER_OF_ITERATIONS = "MAX_NUMBER_OF_ITERATIONS";
    static final String ADD_BODY_CONTENT = "ADD_BODY_CONTENT";
    static final AtomicLong countNumberOfIterations = new AtomicLong(-1);
    private static final Logger LOG = LoggerFactory.getLogger(ParallelHandler.class);

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

        DocumentParserFeature parser = Queries.parserFor(node.splitQuery().getClass());
        List<?> pageList = Queries.from(node.splitQuery()).evaluateList(response.body());

        // add correlation-id before fan-out
        CorrelationIds.of(input).add();

        if (input.state(MAX_NUMBER_OF_ITERATIONS) != null) {
            countNumberOfIterations.incrementAndGet();
        }

        FixedThreadPool threadPool = input.services().get(FixedThreadPool.class);

        PageContext pageContext = buildPageContext(input);

        for (Object pageEntryDocument : pageList) {

            if (input.state(MAX_NUMBER_OF_ITERATIONS) != null) {
                long maxNumberOfIterations = Long.parseLong(input.state(MAX_NUMBER_OF_ITERATIONS).toString());
                if (ParallelHandler.countNumberOfIterations.get() >= maxNumberOfIterations) {
                    pageContext.setEndOfStream(true);
                    break;
                }
            }

            byte[] serializedItem = parser.serialize(pageEntryDocument);

            /*
             * Resolve variables
             */
            node.variableNames().forEach(variableKey -> {
                String value = Queries.from(node.variable(variableKey)).evaluateStringLiteral(pageEntryDocument);
                input.variable(variableKey, value);
            });

            /*
             * execute step nodes
             */

            ExecutionContext nodeInput = ExecutionContext.of(input);
            CompletableFuture<ExecutionContext> parallelFuture = CompletableFuture
                    .supplyAsync(() -> {
                        if (pageContext.isFailure()) {
                            pageContext.setEndOfStream(true);
                            return ExecutionContext.empty();
                        }

                        ExecutionContext accumulated = ExecutionContext.empty();

                        for (Node step : node.steps()) {
                            ExecutionContext stepInput = ExecutionContext.of(nodeInput).merge(accumulated);
                            stepInput.state(PageEntryState.class, new PageEntryState(pageEntryDocument, serializedItem));

                            // set state nestedOperation to true to inform AddContentHandler to buffer response body
                            if (step instanceof Execute) {
                                stepInput.state(ADD_BODY_CONTENT, true);
                            }

                            ExecutionContext stepOutput = Executor.execute(step, stepInput);
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

            pageContext.addFuture(parallelFuture);

            if (ParallelHandler.countNumberOfIterations.get() > -1) {
                ParallelHandler.countNumberOfIterations.incrementAndGet();
            }
        }

        return ExecutionContext.empty().merge(CorrelationIds.of(input).context()).state(PageContext.class, pageContext);
    }

}
