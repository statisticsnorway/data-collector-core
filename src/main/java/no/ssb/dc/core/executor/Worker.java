package no.ssb.dc.core.executor;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Node;

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

}
