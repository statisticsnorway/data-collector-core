package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.HttpStatusCode;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.HttpStatusValidation;

import java.nio.charset.StandardCharsets;

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
            // todo make explicit handling of 3xx redirect, 4xx client error, 5xx server error.
            boolean expectedErrorCodes = node.failed().stream().anyMatch(code -> code.statusCode() == statusCode);
            if (expectedErrorCodes) {
                HttpStatusCode failedStatus = HttpStatusCode.valueOf(statusCode);
                throw new HttpErrorException(String.format("Error dealing with response: %s [%s] %s%n%s", response.url(), failedStatus.statusCode(), failedStatus.reason(), new String(response.body(), StandardCharsets.UTF_8)));
            } else {
                HttpStatusCode failedStatus = HttpStatusCode.valueOf(statusCode);
                throw new HttpErrorException(String.format("Error dealing with response: %s [%s] %s%n%s", response.url(), failedStatus.statusCode(), failedStatus.reason(), new String(response.body(), StandardCharsets.UTF_8)));
            }
        }
        return ExecutionContext.empty();
    }

}
