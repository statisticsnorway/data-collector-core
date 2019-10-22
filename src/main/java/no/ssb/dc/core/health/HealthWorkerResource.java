package no.ssb.dc.core.health;

import com.fasterxml.jackson.annotation.JsonProperty;
import no.ssb.dc.api.health.HealthRenderPriority;
import no.ssb.dc.api.health.HealthResource;
import no.ssb.dc.api.health.HealthResourceExclude;

import java.util.Deque;
import java.util.Map;
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
        return new WorkerInfo(workerId.toString());
    }

    public HealthWorkerMonitor getMonitor() {
        return monitor;
    }

    public static class WorkerInfo {
        @JsonProperty public final String workerId;

        public WorkerInfo(String workerId) {
            this.workerId = workerId;
        }
    }
}
