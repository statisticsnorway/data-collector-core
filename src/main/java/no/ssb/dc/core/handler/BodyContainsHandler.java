package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.BodyContains;
import no.ssb.dc.api.node.ResponsePredicate;

@Handler(forClass = BodyContains.class)
public class BodyContainsHandler extends AbstractResponsePredicateHandler<BodyContains> {

    public BodyContainsHandler(BodyContains node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        Response response = context.state(Response.class);
        if (response == null) {
            return ExecutionContext.empty();
        }

        String queryResult = Queries.from(node.getQuery()).evaluateStringLiteral(response.body());

        Boolean test = node.getEqualToStringLiteral().equals(queryResult);

        return ExecutionContext.empty().state(ResponsePredicate.RESPONSE_PREDICATE_RESULT, test);
    }

}
