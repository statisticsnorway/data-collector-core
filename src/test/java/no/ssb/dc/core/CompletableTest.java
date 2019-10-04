package no.ssb.dc.core;

import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.dc.core.executor.FixedThreadPool;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class CompletableTest {

    final Supplier<String> currentThreadName = () -> Thread.currentThread().getName();

    @Test
    public void testCompletables() throws ExecutionException, InterruptedException {
        DynamicConfiguration configuration = new StoreBasedDynamicConfiguration.Builder()
                .build();

        FixedThreadPool threadPool = FixedThreadPool.newInstance(configuration.evaluateToInt("data.collector.worker.threads"));

        Supplier<String> nestedFuture = () -> {
            String msg = currentThreadName.get();
            return msg;
        };

        CompletableFuture<String> nestedFutureWithExecutor = CompletableFuture.supplyAsync(() -> {
            String msg = currentThreadName.get();
            System.out.printf("HERE 1: %s%n", Thread.currentThread().getName());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return msg;
        }, threadPool.getExecutor())
                .thenApply(s -> {
                    System.out.printf("HERE 2: %s%n", Thread.currentThread().getName());
                    return s;
                });

        Thread.sleep(1500);

//        if (true) return;

        //nestedFutureWithExecutor.get();

        CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
            String msg = currentThreadName.get();
            System.out.printf("supplyAsync: origin: %s -- nested: %s%n", msg, nestedFuture.get());
            return msg;

        }, threadPool.getExecutor()).thenApply(msg -> {
            System.out.printf("apply: origin: %s -- this: %s -- nested: %s%n", msg, currentThreadName.get(), nestedFuture.get());
            nestedFuture.get();
            return msg;

//        }).thenCombineAsync(nestedFuture, (msg, nestedFutureResult) -> {
//            String nestedMsg = nestedFuture.get();
//            System.out.printf("combineAsync: origin: %s -- combine: %s -> %s%n", msg, nestedMsg, nestedFutureResult);
//            return msg;
        }).thenApply(msg -> {
            System.out.printf("apply: origin: %s -- this: %s -- nested: %s%n", msg, currentThreadName.get(), nestedFuture.get());
            nestedFuture.get();
            return msg;
        }).thenRunAsync(() -> {
            String msg = currentThreadName.get();
            System.out.printf("runAsyncWithExector: origin: %s nested: %s%n", msg, nestedFutureWithExecutor.join());
        }, threadPool.getExecutor())
                .thenRunAsync(() -> {
                    String msg = currentThreadName.get();
                    System.out.printf("runAsync: origin: %s nested: %s%n", msg, nestedFutureWithExecutor.join());
                });

        future.join();
    }
}
