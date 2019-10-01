package no.ssb.dc.core.executor;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Node;

public class Worker {

    private final Node node;
    private final ExecutionContext input;

    public Worker(Node node, ExecutionContext input) {
        this.node = node;
        this.input = input;
    }

    public ExecutionContext run() throws InterruptedException {
        ExecutionContext output = Executor.execute(node, input);
        return output;
    }

}
