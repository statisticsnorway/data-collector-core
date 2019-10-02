package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.HttpStatusCode;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.HttpStatusValidation;

@Handler(forClass = HttpStatusValidation.class)
public class HttpStatusValidationHandler extends AbstractHandler<HttpStatusValidation> {

    public HttpStatusValidationHandler(HttpStatusValidation node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        Response response = context.state(Response.class);
        int statusCode = response.statusCode();
        boolean success = node.success().stream().anyMatch(code -> code.statusCode() == statusCode);
        if (!success) {
            boolean failed = node.failed().stream().anyMatch(code -> code.statusCode() == statusCode);
            if (failed) {
                HttpStatusCode failedStatus = HttpStatusCode.valueOf(statusCode);
                throw new RuntimeException(String.format("Error dealing with response: %s -- %s -- %s", failedStatus.statusCode(), failedStatus.reason(), new String(response.body())));
            }
        }
        return ExecutionContext.empty();
    }

}
