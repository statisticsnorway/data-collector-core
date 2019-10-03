package no.ssb.dc.core.executor;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.builder.FlowBuilder;
import no.ssb.dc.api.node.builder.NodeBuilder;
import no.ssb.dc.api.services.Services;

import java.util.LinkedHashMap;
import java.util.Map;

public class Worker {

    private final Node node;
    private final ExecutionContext context;

    public Worker(Node node, ExecutionContext context) {
        this.node = node;
        this.context = context;
    }

    public ExecutionContext run() throws InterruptedException {
        
        ExecutionContext output = Executor.execute(node, context);
        return output;
    }

    public static WorkerBuilder newBuilder() {
        return new WorkerBuilder();
    }

    public static class WorkerBuilder {

        private ExecutionContext.Builder contextBuilder = new ExecutionContext.Builder();
        private FlowBuilder flowBuilder;
        private NodeBuilder nodeBuilder;
        private Services services;
        private String initialPosition;
        private Headers headers = new Headers();
        private Map<String, String> variables = new LinkedHashMap<>();

        public WorkerBuilder flow(FlowBuilder flowBuilder) {
            this.flowBuilder = flowBuilder;
            return this;
        }

        public WorkerBuilder flow(NodeBuilder nodeBuilder) {
            this.nodeBuilder = nodeBuilder;
            return this;
        }

        public WorkerBuilder services(Services services) {
            this.services = services;
            return this;
        }

        public WorkerBuilder initialPosition(String position) {
            this.initialPosition = position;
            return this;
        }

        public WorkerBuilder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        public WorkerBuilder variable(String name, String value) {
            variables.put(name, value);
            return this;
        }

        public Worker build() {
            //contextBuilder.globalState(Headers.class, headers);
            return new Worker(null, null);
        }

    }

}
