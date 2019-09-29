package no.ssb.dc.core.handler;

import no.ssb.dc.api.Execution;
import no.ssb.dc.api.node.BaseNode;

public abstract class AbstractHandler<N extends BaseNode> implements Execution {

    protected final N node;

    public AbstractHandler(N node) {
        this.node = node;
    }

    Class<? extends BaseNode> getSpecializedQueryInterface(BaseNode node) {
        return (Class<? extends BaseNode>) node.getClass().getInterfaces()[0];
    }


}
