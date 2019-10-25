package no.ssb.dc.core.executor;

import no.ssb.dc.api.context.ExecutionContext;

import java.util.UUID;

public class WorkerObservable {
    private final UUID workerId;
    private final String specificationId;
    private final ExecutionContext context;

    public WorkerObservable(UUID workerId, String specificationId, ExecutionContext context) {
        this.workerId = workerId;
        this.specificationId = specificationId;
        this.context = context;
    }

    public UUID workerId() {
        return workerId;
    }

    public String specificationId() {
        return specificationId;
    }

    public ExecutionContext context() {
        return context;
    }
}
