package no.ssb.dc.core.executor;

import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.core.handler.AbstractHandler;
import no.ssb.dc.core.handler.Handlers;
import no.ssb.dc.core.handler.PaginateHandler;
import no.ssb.rawdata.api.RawdataProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(Lifecycle.class);

    final FixedThreadPool threadPool;
    final RawdataProducer producer;

    public Lifecycle(FixedThreadPool threadPool, RawdataProducer producer) {
        this.threadPool = threadPool;
        this.producer = producer;
    }

    /*
     *
     * 1) PaginateHandler.executeAsync -> 2) Worker.run() -> 3) Lifecycle.execute() -> 4) Lifecycle.executePaginationHandler()
     *
     * Run Worker and dispatch Lifecycle.execute that executes PaginationHandler.execute() that invokes
     * SequenceHandler and NextHandler, which do passes expectedPositions and nextPagePostion to Lifecycle callbacks: doPageSequence and doNextPage.
     * PaginationHandler.execute() executes Parallel and produces deferred itemListItem futures, which will execute GetHandler and PublishHandler.
     *
     */
    public void execute(Interfaces.Paginate paginate, ExecutionContext input) {
        PaginateHandler paginationHandler = ((PaginateHandler) (AbstractHandler<?>) Handlers.createHandlerFor(paginate));
        paginationHandler.executeWork(input);
    }
}
