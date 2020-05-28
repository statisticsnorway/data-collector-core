package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.HttpStatusCode;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.HttpStatusValidation;
import no.ssb.dc.api.node.ResponsePredicate;
import no.ssb.dc.core.executor.Executor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Handler(forClass = HttpStatusValidation.class)
public class HttpStatusValidationHandler extends AbstractHandler<HttpStatusValidation> {

    public HttpStatusValidationHandler(HttpStatusValidation node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        Response response = context.state(Response.class);
        int statusCode = response.statusCode();

        boolean success = false;
        for (Map.Entry<HttpStatusCode, List<ResponsePredicate>> entry : node.success().entrySet()) {
            // true when statusCode in success list and no predicates should be evaluated
            if (entry.getKey().statusCode() == statusCode && entry.getValue().isEmpty()) {
                success = true;
                break;
            }

            // evaluate predicates
            if (entry.getKey().statusCode() == statusCode) {
                for (ResponsePredicate responsePredicate : entry.getValue()) {
                    // response predicate handler must evaluate state(Response.class).body
                    ExecutionContext output = Executor.execute(responsePredicate, ExecutionContext.of(context));
                    boolean test = output.state(ResponsePredicate.RESPONSE_PREDICATE_RESULT);
                    if (!test) {
                        break;
                    }
                }
                success = true;
                break;
            }
        }

        // TODO remove failure. Either we got a success, or it is a failure.
        if (!success) {
            // todo make explicit handling of 3xx redirect, 4xx client error, 5xx server error.
            boolean expectedErrorCodes = node.failed().stream().anyMatch(code -> statusCode == code.statusCode());
            if (expectedErrorCodes) {
                throwHttpErrorException(statusCode, response);
            }
            throwHttpErrorException(statusCode, response);
        }

        return ExecutionContext.empty();
    }

    void throwHttpErrorException(int statusCode, Response response) {
        HttpStatusCode failedStatus = HttpStatusCode.valueOf(statusCode);
        throw new HttpErrorException(String.format("Error dealing with response: %s [%s] %s%n%s",
                response.url(), failedStatus.statusCode(), failedStatus.reason(), new String(response.body(), StandardCharsets.UTF_8)));

    }

}
