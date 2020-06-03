package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
@Handler(forClass = Get.class)
public class GetHandler extends AbstractOperationHandler<Get> {

    private static final Logger LOG = LoggerFactory.getLogger(GetHandler.class);

    public GetHandler(Get node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        int requestTimeout = beforeRequest(input);

        Request.Builder requestBuilder = Request.newRequestBuilder().GET(); // . timeout(Duration.ofSeconds(requestTimeout));

        Response response = doRequest(input, requestTimeout, requestBuilder);

        return handleResponse(input, response);
    }

}