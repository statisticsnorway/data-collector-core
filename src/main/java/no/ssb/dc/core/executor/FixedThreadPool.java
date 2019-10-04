package no.ssb.dc.core.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FixedThreadPool {

    private final static Logger LOG = LoggerFactory.getLogger(FixedThreadPool.class);

    private final ExecutorService fixedThreadPool;

    private FixedThreadPool(int numberOfThreads) {
        this.fixedThreadPool = createFixedThreadPool(numberOfThreads);
    }
    public static FixedThreadPool newInstance() {
        return new FixedThreadPool(-1);
    }

    public static FixedThreadPool newInstance(int numberOfThreads) {
        return new FixedThreadPool(numberOfThreads);
    }

    static ExecutorService createFixedThreadPool(int numberOfThreads) {
        int configuredThreadCount = numberOfThreads == -1 ? getNumberOfThreads() : numberOfThreads;
        LOG.info("Number of worker threads set to: {}", configuredThreadCount);
        return Executors.newFixedThreadPool(configuredThreadCount);
    }

    public static int getNumberOfThreads() {
        return Runtime.getRuntime().availableProcessors() + 2;
    }

    public ExecutorService getExecutor() {
        return fixedThreadPool;
    }

    public void shutdownAndAwaitTermination() {
        fixedThreadPool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!fixedThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                fixedThreadPool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!fixedThreadPool.awaitTermination(60, TimeUnit.SECONDS))
                    LOG.error("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            fixedThreadPool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

    }


}
