package no.ssb.dc.core.executor;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class WorkerObserver {

    private final Consumer<WorkerObservable> onStartCallback;
    private final BiConsumer<WorkerObservable, WorkerOutcome> onFinishCallback;

    public WorkerObserver(Consumer<WorkerObservable> onStartCallback, BiConsumer<WorkerObservable, WorkerOutcome> onFinishCallback) {
        this.onStartCallback = onStartCallback;
        this.onFinishCallback = onFinishCallback;
    }

    public void start(WorkerObservable observable) {
        onStartCallback.accept(observable);
    }

    public void finish(WorkerObservable observable, WorkerOutcome outcome) {
        onFinishCallback.accept(observable, outcome);
    }
}
