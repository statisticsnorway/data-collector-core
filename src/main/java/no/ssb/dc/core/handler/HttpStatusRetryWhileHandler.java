package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.error.ExecutionException;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.HttpStatusRetryWhile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Handler(forClass = HttpStatusRetryWhile.class)
public class HttpStatusRetryWhileHandler extends AbstractHandler<HttpStatusRetryWhile> {

    static final Logger LOG = LoggerFactory.getLogger(HttpStatusRetryWhileHandler.class);

    static String TRY_AGAIN = "TRY_AGAIN";

    public HttpStatusRetryWhileHandler(HttpStatusRetryWhile node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        Request request = context.state(Request.class);
        Response response = context.state(Response.class);
        if (response.statusCode() == node.statusCode()) {
            try {
                LOG.trace("Retry in {} {} cause {} @ {}", node.amount(), node.duration().name().toLowerCase(), new String(response.body()), request.url());
                node.duration().sleep(node.amount());
                return ExecutionContext.empty().state(TRY_AGAIN, true);

            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            }
        }
        return ExecutionContext.empty().state(TRY_AGAIN, false);
    }
}
