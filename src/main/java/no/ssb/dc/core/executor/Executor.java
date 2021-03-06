package no.ssb.dc.core.executor;

import no.ssb.dc.api.Execution;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Base;
import no.ssb.dc.core.handler.Handlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor {

    private static final Logger LOG = LoggerFactory.getLogger(Executor.class);

    public static <N extends Base> Execution instanceOf(N node) {
        return Handlers.createHandlerFor(node);
    }

    public static <N extends Base> ExecutionContext execute(N node, ExecutionContext input) {
        Execution executionHandler = Handlers.createHandlerFor(node);
        return executionHandler.execute(input);
    }

}
