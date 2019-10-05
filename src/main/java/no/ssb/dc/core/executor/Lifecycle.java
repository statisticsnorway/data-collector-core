package no.ssb.dc.core.executor;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.core.handler.Conditions;
import no.ssb.dc.core.handler.PaginateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(Lifecycle.class);

    private final PaginateHandler paginateHandler;

    public Lifecycle(PaginateHandler paginateHandler) {
        this.paginateHandler = paginateHandler;
    }

    public ExecutionContext execute(ExecutionContext context) {
        context.state(Lifecycle.class, this);

        ExecutionContext loopOutput;
        do {
            loopOutput = paginateHandler.doPage(context);
        } while (Conditions.untilCondition(paginateHandler.node().condition(), loopOutput));

        LOG.info("Paginate has completed!");

        return ExecutionContext.empty();
    }
}
