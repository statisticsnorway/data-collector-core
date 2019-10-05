package no.ssb.dc.core.executor;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Paginate;
import no.ssb.dc.core.handler.AbstractHandler;
import no.ssb.dc.core.handler.Handlers;
import no.ssb.dc.core.handler.PaginateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(Lifecycle.class);

    final FixedThreadPool threadPool;

    public Lifecycle(FixedThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void execute(Paginate paginate, ExecutionContext input) {
        PaginateHandler paginationHandler = ((PaginateHandler) (AbstractHandler<?>) Handlers.createHandlerFor(paginate));
        paginationHandler.doPage(input);
    }
}
