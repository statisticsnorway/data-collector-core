package no.ssb.dc.core.handler;

import no.ssb.dc.api.Execution;
import no.ssb.dc.api.node.Base;

public abstract class AbstractHandler<N extends Base> implements Execution {

    protected final N node;

    public AbstractHandler(N node) {
        this.node = node;
    }

    Class<? extends Base> getSpecializedQueryInterface(Base node) {
        return (Class<? extends Base>) node.getClass().getInterfaces()[0];
    }


}
