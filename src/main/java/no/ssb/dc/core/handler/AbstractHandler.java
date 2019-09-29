package no.ssb.dc.core.handler;

import no.ssb.dc.api.Execution;
import no.ssb.dc.api.Interfaces;

public abstract class AbstractHandler<N extends Interfaces.BaseNode> implements Execution {

    protected final N node;

    public AbstractHandler(N node) {
        this.node = node;
    }

    Class<? extends Interfaces.BaseNode> getSpecializedQueryInterface(Interfaces.BaseNode node) {
        return (Class<? extends Interfaces.BaseNode>) node.getClass().getInterfaces()[0];
    }


}
