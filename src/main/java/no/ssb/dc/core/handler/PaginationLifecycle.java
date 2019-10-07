package no.ssb.dc.core.handler;

import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.PageThresholdEvent;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.FixedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class PaginationLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(PaginationLifecycle.class);

    private final PaginateHandler paginateHandler;
    private final AtomicReference<PageContext> lastPageRef = new AtomicReference<>();
    private final BlockingQueue<PageContext> pageContexts = new LinkedBlockingDeque<>();
    private final AtomicReference<Throwable> failedException = new AtomicReference<>();

    PaginationLifecycle(PaginateHandler paginateHandler) {
        this.paginateHandler = paginateHandler;
    }

    CompletableFuture<PageContext> preFetchPage(ExecutionContext context, FixedThreadPool threadPool) {
        return CompletableFuture
                .supplyAsync(() -> {

                    /*
                     * Sequence-description:
                     *
                     * A PageContextBuilder is created in GetHandler
                     *  GetHandler executes SequenceHandler and NextPageHandler and collects expectedSequence and next-page variables to PageContextBuilder
                     *  GetHandler executes ParallelHandler and builds a PageContext and collects futures to PageContext.futures
                     *  ParallelHandler evaluates if page threshold is met and fires preFetchNextPageCallback that creates (copy) of input context and calls preFetchPage (traversal)
                     * Note: The context-variable passed on paginateHandler.doPage (below) are operating on the same context as the callback function
                     */
                    Consumer<PageContext> preFetchNextPageCallback = pageContext -> {
                        LOG.info("Pre-fetch threshold: {}, next-page: {}", pageContext.expectedPositions().get(pageContext.completionInfo().completedCount().intValue()), pageContext.nextPositionMap());
                        ExecutionContext nextPageContext = ExecutionContext.of(context);
                        CompletableFuture<PageContext> nextPageFuture = preFetchPage(nextPageContext, threadPool);
                        handleException(nextPageFuture);
                    };

                    // get next page
                    context.state(PageThresholdEvent.class, new PageThresholdEvent(paginateHandler.node.threshold(), preFetchNextPageCallback));
                    ExecutionContext output = paginateHandler.doPage(context);

                    // get page context that contains all page-entry futures
                    PageContext pageContext = output.state(PageContext.class);
                    if (pageContext == null) {
                        PageContext endOfStream = PageContext.createEndOfStream();
                        lastPageRef.set(endOfStream);
                        return endOfStream;
                    }

                    pageContexts.add(pageContext);

                    lastPageRef.set(pageContext);

                    return pageContext;
                }, threadPool.getExecutor())
                .exceptionally(throwable -> {
                    // todo code review of exception handling
                    if (!failedException.compareAndSet(null, throwable)) {
                        LOG.error("Unable to store throwable in failedException, already set. Current exception: {}", CommonUtils.captureStackTrace(throwable));
                    }

                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }
                    throw new RuntimeException(throwable);
                });
    }

    ExecutionContext start(ExecutionContext context) throws InterruptedException {
        context.state(PaginationLifecycle.class, this);
        FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);

        AtomicBoolean initializeStream = new AtomicBoolean(true);
        AtomicBoolean endOfStream = new AtomicBoolean(false);

        while (!endOfStream.get()) {
            if (initializeStream.get()) {
                initializeStream.set(false);
                CompletableFuture<PageContext> firstPageFuture = preFetchPage(context, threadPool);
                handleException(firstPageFuture);
            }

            PageContext pageContext = pageContexts.poll(1, TimeUnit.SECONDS);

            if (pageContext == null && lastPageRef.get() == null) {
                // todo code review of exception handling
                if (failedException.get() == null) {
                    throw new RuntimeException("Unknown and unhandled excpetion occurred!");
                }
                throw new RuntimeException(failedException.get());
            }

            if (pageContext == null && !lastPageRef.get().isEndOfStream()) {
                continue;
            }

            if (pageContext == null && lastPageRef.get().isEndOfStream()) {
                break;
            }

            if (pageContext == null) {
                throw new RuntimeException("Something went wrong during endOfStream!");
            }

            ExecutionContext conditionContext = ExecutionContext.empty();
            String nextPositionVariableName = paginateHandler.node.condition().identifier();
            if (nextPositionVariableName != null) {
                conditionContext = conditionContext.variable(nextPositionVariableName, pageContext.nextPositionMap().get(nextPositionVariableName));
            }
            if (!Conditions.untilCondition(paginateHandler.node.condition(), conditionContext)) {
                endOfStream.set(true);
            }

            CompletableFuture<PageContext> futures = CompletableFuture
                    .allOf(pageContext.parallelFutures().toArray(new CompletableFuture[0]))
                    .thenApply(v -> pageContext)
                    .thenApply(pc -> {
                                LOG.info("Page completion at: {}", pageContext.expectedPositions().get(pageContext.completionInfo().completedCount().intValue() - 1));
                                return pc;
                            }
                    ).exceptionally(throwable -> {
                        // todo code review of exception handling
                        endOfStream.set(true);
                        failedException.compareAndSet(null, throwable);
                        throw new RuntimeException(CommonUtils.captureStackTrace(throwable));
                    });
            futures.join();
        }

        LOG.info("Paginate has completed!");

        return ExecutionContext.empty();
    }

    private void handleException(CompletableFuture<PageContext> nextPageFuture) {
        // todo code review of exception handling
        nextPageFuture.exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        });
    }

}
