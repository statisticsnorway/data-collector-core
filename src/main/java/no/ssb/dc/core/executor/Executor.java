package no.ssb.dc.core.executor;

import no.ssb.dc.api.Execution;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.handler.EndOfStreamException;
import no.ssb.dc.core.handler.Handlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor {

    private static final Logger LOG = LoggerFactory.getLogger(Executor.class);

    public static <N extends Interfaces.BaseNode> Execution instanceOf(N node) {
        return Handlers.createHandlerFor(node);
    }

    public static <N extends Interfaces.BaseNode> ExecutionContext execute(N node, ExecutionContext input) {
        Execution executionHandler = Handlers.createHandlerFor(node);
        try {
            return executionHandler.execute(input);
        } catch (Exception e) {
            if (!(e instanceof EndOfStreamException)) {
                LOG.error("node: {} => {}", node, input, CommonUtils.captureStackTrace(e));
            }
            throw e;
        }
    }

}
