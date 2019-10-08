package no.ssb.dc.core.handler;

import no.ssb.dc.api.PositionObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;

public class PrefetchAlgorithmTest {

    static final Logger LOG = LoggerFactory.getLogger(PrefetchAlgorithmTest.class);

    @Test
    public void thatPrefetchAlgorithWorks() throws InterruptedException {
        final BlockingDeque<CompletableFuture<Integer>> pageCompletedFutures = new LinkedBlockingDeque<>();
        final CountDownLatch completedSignal = new CountDownLatch(1);
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final int MAX_PAGES = 10;
        final int pageSize = 10;
        final AtomicInteger prefetchIdGenerator = new AtomicInteger();
        final AtomicReference<PositionObserver> positionObserverRef = new AtomicReference<>();
        final Supplier<CompletableFuture<Integer>> prefetchSupplier = () -> CompletableFuture.supplyAsync(() -> {
            int prefetchId = prefetchIdGenerator.incrementAndGet();
            if (prefetchId > MAX_PAGES) {
                completedSignal.countDown();
                return prefetchId; // end-of-stream
            }
            PositionObserver positionObserver = positionObserverRef.get();
            LOG.trace("PageId: {}, expected: {}", prefetchId, pageSize);
            positionObserver.expected(pageSize);
            CompletableFuture[] futures = new CompletableFuture[pageSize];
            for (int i = 0; i < pageSize; i++) {
                futures[i] = CompletableFuture.runAsync(() -> {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    try {
                        Thread.sleep(1 + random.nextInt(50));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    LOG.trace("PageId: {}, completed: {}", prefetchId, 1);
                    positionObserver.completed(1);
                }, executor);
            }
            CompletableFuture.allOf(futures).join();
            return prefetchId;
        }, ForkJoinPool.commonPool());

        PrefetchAlgorithm prefetchAlgorithm = new PrefetchAlgorithm(15, () -> {
            pageCompletedFutures.add(prefetchSupplier.get());
        });
        PositionObserver positionObserver = prefetchAlgorithm.getPositionObserver();
        positionObserverRef.set(positionObserver);

        pageCompletedFutures.add(prefetchSupplier.get()); // first fetch

        CompletableFuture<Integer> pageCompletedFuture = pageCompletedFutures.poll(5, TimeUnit.SECONDS);
        int pagesCompleted = 0;
        while (pageCompletedFuture != null) {
            Integer pageId = pageCompletedFuture.join();
            pagesCompleted++;
            LOG.info("page completed: id={}", pageId);
            pageCompletedFuture = pageCompletedFutures.poll(2, TimeUnit.SECONDS);
        }

        assertEquals(pagesCompleted, 11); // 10 pages + 1 end-of-stream marker page
    }

}