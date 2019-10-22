package no.ssb.dc.core.executor;

import no.ssb.dc.api.context.ExecutionContext;

import java.util.UUID;

public class WorkerObservable {
    private final UUID workerId;
    private final ExecutionContext context;

    public WorkerObservable(UUID workerId, ExecutionContext context) {
        this.workerId = workerId;
        this.context = context;
    }

    public UUID workerId() {
        return workerId;
    }

    public ExecutionContext context() {
        return context;
    }
}
