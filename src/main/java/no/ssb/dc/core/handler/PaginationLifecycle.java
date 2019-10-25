package no.ssb.dc.core.handler;

import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.PositionObserver;
import no.ssb.dc.api.Termination;
import no.ssb.dc.api.TerminationException;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.core.executor.FixedThreadPool;
import no.ssb.dc.core.health.HealthWorkerMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class PaginationLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(PaginationLifecycle.class);

    private final int prefetchThreshold;
    private final PaginateHandler paginateHandler;
    private final BlockingDeque<CompletableFuture<ExecutionContext>> pageFutures = new LinkedBlockingDeque<>();
    private final AtomicReference<CompletableFuture<ExecutionContext>> lastPageFuture = new AtomicReference<>();

    PaginationLifecycle(int prefetchThreshold, PaginateHandler paginateHandler) {
        this.prefetchThreshold = prefetchThreshold;
        this.paginateHandler = paginateHandler;
    }

    private Runnable prefetchUntilConditionSatisfiedOrEndOfStream(FixedThreadPool threadPool) {
        return () -> lastPageFuture.get().thenAccept(output -> {

            PageContext pageContext = output.state(PageContext.class);
            if (pageContext.isEndOfStream()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("EOS Prefetching... {}", output.variable(paginateHandler.node.condition().identifier()));
                }
                return; // do not pre-fetch
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Prefetching... {}", output.variable(paginateHandler.node.condition().identifier()));
                }
            }

            CompletableFuture<ExecutionContext> future = preFetchPage(ExecutionContext.of(output), threadPool);

            LOG.trace("Added prefetch: {}", output.variable(paginateHandler.node.condition().identifier()));
            pageFutures.add(future);
            lastPageFuture.set(future);
        });
    }

    private CompletableFuture<ExecutionContext> preFetchPage(ExecutionContext context, FixedThreadPool threadPool) {
        return CompletableFuture
                .supplyAsync(() -> {
                    LOG.info("Pre-fetching next-page. Variables: {}", context.variables());

                    checkTerminationSignal(context.services().get(Termination.class));

                    // increment monitor prefetch count
                    if (context.services().contains(HealthWorkerMonitor.class)) {
                        context.services().get(HealthWorkerMonitor.class).request().incrementPrefetchCount();
                    }

                    // get next page
                    ExecutionContext pageContext = ExecutionContext.of(context);
                    ExecutionContext output = paginateHandler.doPage(pageContext);

                    checkTerminationSignal(context.services().get(Termination.class));

                    if (output.state(PageContext.class) == null) {
                        output.state(PageContext.class, PageContext.createEndOfStream());
                    }

                    return output;
                }, threadPool.getExecutor());
    }

    ExecutionContext start(ExecutionContext context) throws InterruptedException {
        FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);
        Termination termination = context.services().get(Termination.class);
        HealthWorkerMonitor monitor = context.services().get(HealthWorkerMonitor.class);

        PrefetchAlgorithm prefetchAlgorithm = new PrefetchAlgorithm(prefetchThreshold, prefetchUntilConditionSatisfiedOrEndOfStream(threadPool), termination, monitor);
        context.state(PositionObserver.class, prefetchAlgorithm.getPositionObserver());

        doStartAsync(context);

        monitorLifecycleUntilLoopConditionIsSatisfied(termination, monitor);

        return ExecutionContext.empty();
    }

    private void doStartAsync(ExecutionContext context) {
        FixedThreadPool threadPool = context.services().get(FixedThreadPool.class);

        CompletableFuture<ExecutionContext> future = preFetchPage(context, threadPool);
        pageFutures.add(future);
        lastPageFuture.set(future);
    }

    private void monitorLifecycleUntilLoopConditionIsSatisfied(Termination termination, HealthWorkerMonitor monitor) throws InterruptedException {
        boolean untilCondition = false;

        do {

            CompletableFuture<ExecutionContext> pageFuture = pageFutures.poll(1, TimeUnit.SECONDS);

            if (pageFuture == null) {
                continue;
            }

            checkTerminationSignal(termination);

            ExecutionContext outputContext = pageFuture.join();

            PageContext pageContext = outputContext.state(PageContext.class);

            // TODO wait for all tasks in parallel-handler instead of here
            CompletableFuture.allOf(pageContext.parallelFutures().toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        // decrement monitor prefetch count
                        if (monitor != null) {
                            monitor.request().decrementPrefetchCount();
                        }
                        return v;
                    })
                    .join();

            if (pageContext.isEndOfStream()) {
                LOG.info("Termination page!");

            } else {
                LOG.info("Page completion at: {}", pageContext.expectedPositions().get(pageContext.completionInfo().completedCount().intValue() - 1));
            }

            if (pageContext.isEndOfStream()) {
                break;
            }

            untilCondition = evaluateUntilCondition(pageContext);

        } while (!untilCondition);

        LOG.info("Paginate has completed!");
    }

    private void checkTerminationSignal(Termination termination) {
        if (termination != null && termination.isTerminated()) {
            throw new TerminationException();
        }
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
