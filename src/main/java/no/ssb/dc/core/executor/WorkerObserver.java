package no.ssb.dc.core.executor;

import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class WorkerObserver {

    private final Consumer<UUID> onStartCallback;
    private final BiConsumer<UUID, WorkerOutcome> onFinishCallback;

    public WorkerObserver(Consumer<UUID> onStartCallback, BiConsumer<UUID, WorkerOutcome> onFinishCallback) {
        this.onStartCallback = onStartCallback;
        this.onFinishCallback = onFinishCallback;
    }

    public void start(UUID workerId) {
        onStartCallback.accept(workerId);
    }

    public void finish(UUID workerId, WorkerOutcome outcome) {
        onFinishCallback.accept(workerId, outcome);
    }
}
