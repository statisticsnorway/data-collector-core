package no.ssb.dc.core.handler;

import no.ssb.dc.api.Execution;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Base;
import no.ssb.dc.api.node.Node;

public abstract class AbstractHandler<N extends Base> implements Execution {

    protected final N node;

    public AbstractHandler(N node) {
        if (node instanceof Node && !(this instanceof AbstractNodeHandler)) {
            throw new RuntimeException(node.getClass() + " is of type Node and should inherit AbstractNodeHandler and successor should invoke super.execute(input) in order to guarantee a configured context!");
        }
        this.node = node;
    }

    Class<? extends Base> getSpecializedQueryInterface(Base node) {
        return (Class<? extends Base>) node.getClass().getInterfaces()[0];
    }

    ExecutionContext configureContext(ExecutionContext context) {
        if (!(node instanceof Node)) {
            throw new IllegalStateException("You are not allowed to configure a context for node type: " + node);
        }
        Node configureNode = (Node) node;
        ExecutionContext globalContext = configureNode.configurations().flowContext().globalContext();
        context.join(globalContext);

        return context;
    }

}
