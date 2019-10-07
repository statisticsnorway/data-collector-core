package no.ssb.dc.core.handler;

import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.PositionObserver;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.core.executor.FixedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class PaginationLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(PaginationLifecycle.class);

    private final PaginateHandler paginateHandler;
    private final BlockingDeque<CompletableFuture<PageContext>> pageFutures = new LinkedBlockingDeque<>();
    private final AtomicReference<CompletableFuture<PageContext>> lastPageFuture = new AtomicReference<>();

    PaginationLifecycle(PaginateHandler paginateHandler) {
        this.paginateHandler = paginateHandler;
    }

    CompletableFuture<PageContext> preFetchPage(ExecutionContext context, FixedThreadPool threadPool) {
        return CompletableFuture
                .supplyAsync(() -> {

                    // get next page
                    ExecutionContext output = paginateHandler.doPage(context);

                    // get page context that contains all page-entry futures
                    PageContext pageContext = output.state(PageContext.class);
                    if (pageContext == null) {
                        PageContext endOfStream = PageContext.createEndOfStream();
                        return endOfStream;
                    }

                    return pageContext;
                }, threadPool.getExecutor());
    }

    ExecutionContext start(ExecutionContext context) throws InterruptedException {
        FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);

        AtomicLong outstandingPositionCounter = new AtomicLong();
        AtomicLong positionCompletedCounter = new AtomicLong();

        Consumer<Integer> expectedCallback = expectedCount -> {
            outstandingPositionCounter.addAndGet(expectedCount);
        };

        Consumer<Integer> positionCompleted = completedCount -> {

            long totalCompletedCount = positionCompletedCounter.addAndGet(completedCount);

            boolean belowThreshold = false;
            if (belowThreshold) {

                // TODO

                //LOG.info("Pre-fetch threshold: {}, next-page: {}", pageContext.expectedPositions().get(pageContext.completionInfo().completedCount().intValue()), pageContext.nextPositionMap());
                ExecutionContext nextPageContext = ExecutionContext.of(context);
                CompletableFuture<PageContext> future = preFetchPage(nextPageContext, threadPool);
                pageFutures.add(future);
                lastPageFuture.set(future);
            }
        };

        context.state(PositionObserver.class, new PositionObserver(expectedCallback, positionCompleted));

        doStartAsync(context);

        monitorLifecycleUntilLoopConditionIsSatisfied();

        return ExecutionContext.empty();
    }

    private void doStartAsync(ExecutionContext context) {
        context.state(PaginationLifecycle.class, this);
        FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);

        CompletableFuture<PageContext> future = preFetchPage(context, threadPool);
        pageFutures.add(future);
        lastPageFuture.set(future);
    }

    private void monitorLifecycleUntilLoopConditionIsSatisfied() throws InterruptedException {
        boolean untilCondition = false;

        do {

            CompletableFuture<PageContext> pageContextFuture = pageFutures.poll(1, TimeUnit.SECONDS);

            if (pageContextFuture == null) {
                continue;
            }

            PageContext pageContext = pageContextFuture.join();

            CompletableFuture.allOf(pageContext.parallelFutures().toArray(new CompletableFuture[0]))
                    .join();

            LOG.info("Page completion at: {}", pageContext.expectedPositions().get(pageContext.completionInfo().completedCount().intValue() - 1));

            if (pageContext.isEndOfStream()) {
                break;
            }

            untilCondition = evaluateUntilCondition(pageContext);

        } while (!untilCondition);

        LOG.info("Paginate has completed!");
    }

    private boolean evaluateUntilCondition(PageContext pageContext) {
        ExecutionContext conditionContext = ExecutionContext.empty();
        String nextPositionVariableName = paginateHandler.node.condition().identifier();
        if (nextPositionVariableName != null) {
            conditionContext = conditionContext.variable(nextPositionVariableName, pageContext.nextPositionMap().get(nextPositionVariableName));
        }
        return Conditions.untilCondition(paginateHandler.node.condition(), conditionContext);
    }
}
