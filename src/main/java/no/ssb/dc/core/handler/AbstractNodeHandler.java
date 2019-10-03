package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Node;

public abstract class AbstractNodeHandler<T extends Node> extends AbstractHandler<T> {

    public AbstractNodeHandler(T node) {
        super(node);
    }

    /**
     * Returns an enriched context with global configuration applied
     *
     * @param context input context
     * @return input context
     */
    @Override
    public ExecutionContext execute(ExecutionContext context) {
        return configureContext(context);
    }
}
