package no.ssb.dc.core.handler;

import no.ssb.dc.api.PositionObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class PrefetchAlgorithm {

    static final Logger LOG = LoggerFactory.getLogger(PrefetchAlgorithm.class);

    final int prefetchThreshold;
    final Runnable prefetchRunnable;

    final AtomicLong expectedPositionCounter = new AtomicLong();
    final AtomicLong positionCompletedCounter = new AtomicLong();
    final AtomicLong pendingPrefetches = new AtomicLong(1);

    public PrefetchAlgorithm(int prefetchThreshold, Runnable prefetchRunnable) {
        this.prefetchThreshold = prefetchThreshold;
        this.prefetchRunnable = prefetchRunnable;
    }

    public PositionObserver getPositionObserver() {
        return new PositionObserver(expectedConsumerFunction(), completedConsumerFunction());
    }

    private Consumer<Integer> expectedConsumerFunction() {
        return expectedCount -> {
            long total = expectedPositionCounter.addAndGet(expectedCount);
            LOG.trace("expected observed: added={}, total={}", expectedCount, total);
            long countAfterDecrement = pendingPrefetches.decrementAndGet();
            if (countAfterDecrement < 0) {
                throw new IllegalStateException("count-after-decrement < 0");
            }

            prefetchIfBelowThreshold(expectedPositionCounter.get() - positionCompletedCounter.get());
        };
    }

    private Consumer<Integer> completedConsumerFunction() {
        return completedCount -> {
            long totalCompletedCount = positionCompletedCounter.addAndGet(completedCount);
            LOG.trace("completed observed: added={}, total={}", completedCount, totalCompletedCount);

            if (pendingPrefetches.get() > 0) {
                return; // threshold will be checked when expected counter is increased
            }

            prefetchIfBelowThreshold(expectedPositionCounter.get() - totalCompletedCount);
        };
    }

    private void prefetchIfBelowThreshold(long pendingPositions) {
        if (pendingPositions < prefetchThreshold) {

            if (!pendingPrefetches.compareAndSet(0, 1)) {
                return; // a concurrent thread won the race to start a pre-fetch
            }

            LOG.trace("Pre-fetching next-page...");
            prefetchRunnable.run();
        }
    }
}
