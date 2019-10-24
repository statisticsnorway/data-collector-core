package no.ssb.dc.core.health;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import no.ssb.dc.api.health.HealthRenderPriority;
import no.ssb.dc.api.health.HealthResource;
import no.ssb.dc.api.health.HealthResourceExclude;
import no.ssb.dc.core.executor.WorkerStatus;

import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@HealthRenderPriority(priority = 10)
@HealthResourceExclude
public class HealthWorkerResource implements HealthResource {

    private final UUID workerId;
    private final HealthWorkerMonitor monitor;

    public HealthWorkerResource(UUID workerId) {
        this.workerId = workerId;
        monitor = new HealthWorkerMonitor();
    }

    @Override
    public Optional<Boolean> isUp() {
        return Optional.of(monitor.contentStream().monitor().isUp());
    }

    @Override
    public String name() {
        return "worker-" + workerId.toString();
    }

    @Override
    public boolean isList() {
        return false;
    }

    @Override
    public boolean canRender(Map<String, Deque<String>> queryParams) {
        return true;
    }

    @Override
    public Object resource() {
        return new WorkerResource(workerId.toString(), monitor.build());
    }

    public HealthWorkerMonitor getMonitor() {
        return monitor;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"status", "worker-id"})
    @SuppressWarnings("WeakerAccess")
    public static class WorkerResource {
        @JsonProperty("status") public final WorkerStatus status;
        @JsonProperty("worker-id") public final String workerId;
        @JsonUnwrapped public final HealthWorkerMonitor.WorkerInfo workerInfo;

        public WorkerResource(String workerId, HealthWorkerMonitor.WorkerInfo workerInfo) {
            this.workerId = workerId;
            this.workerInfo = workerInfo;
            this.status = workerInfo.status;
        }
    }

}
