package no.ssb.dc.core.executor;

import no.ssb.config.DynamicConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FixedThreadPool {

    private final static Logger LOG = LoggerFactory.getLogger(FixedThreadPool.class);

    private final DynamicConfiguration configuration;
    private final ExecutorService fixedThreadPool;

    private FixedThreadPool(DynamicConfiguration configuration) {
        this.configuration = configuration;
        this.fixedThreadPool = createFixedThreadPool(configuration);
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

    public static FixedThreadPool newInstance(DynamicConfiguration configuration) {
        return new FixedThreadPool(configuration);
    }

    static ExecutorService createFixedThreadPool(DynamicConfiguration configuration) {
        int numberOfThreads = (configuration.evaluateToString("data.collector.worker.threads") == null ? getNumberOfThreads() : configuration.evaluateToInt("data.collector.worker.threads"));
        LOG.info("Number of worker threads set to: {}", numberOfThreads);
        return Executors.newFixedThreadPool(numberOfThreads);
    }

    public static int getNumberOfThreads() {
        return Runtime.getRuntime().availableProcessors() + 2;
    }


}
