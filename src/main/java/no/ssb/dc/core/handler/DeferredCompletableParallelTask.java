package no.ssb.dc.core.handler;

import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.executor.FixedThreadPool;

import java.util.concurrent.CompletableFuture;

public class DeferredCompletableParallelTask {

    public final Position<?> entryPosition;
    public final Interfaces.Execute execute;
    public final ExecutionContext executeInput;
    public final Interfaces.Publish publish;

    public DeferredCompletableParallelTask(Position<?> entryPosition, Interfaces.Execute execute, ExecutionContext executeInput, Interfaces.Publish publish) {
        this.entryPosition = entryPosition;
        this.execute = execute;
        this.executeInput = executeInput;
        this.publish = publish;
    }

    public CompletableFuture<ExecutionContext> getFuture() {
        FixedThreadPool threadPool = executeInput.services().get(FixedThreadPool.class);
        return CompletableFuture.supplyAsync(() -> Executor.execute(execute, executeInput), threadPool.getExecutor())
                .thenApply(executeOutput -> {
                    if (publish != null) {
                        ExecutionContext publishOutput = Executor.execute(publish, executeInput);
                        // no state/variable copying
                    }
                    return executeOutput;
                });
    }

}
