package no.ssb.dc.core.handler;

import no.ssb.dc.api.node.Base;

public abstract class AbstractResponsePredicateHandler<N extends Base> extends AbstractHandler<N> {

    public AbstractResponsePredicateHandler(N node) {
        super(node);
    }

}
