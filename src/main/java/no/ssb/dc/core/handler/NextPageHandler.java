package no.ssb.dc.core.handler;

import no.ssb.dc.api.PositionProducer;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.NextPage;
import no.ssb.dc.api.node.Query;

import java.util.Map;

@Handler(forClass = NextPage.class)
public class NextPageHandler extends AbstractHandler<NextPage> {

    public NextPageHandler(NextPage node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.empty();

        Response response = input.state(Response.class);
        byte[] body = response.body();

        PositionProducer<?> positionProducer = input.state(PositionProducer.class);

        for (Map.Entry<String, Query> entry : node.outputs().entrySet()) {
            String variableName = entry.getKey();
            Query variableQuery = entry.getValue();
            String variableValue = Queries.evaluate(variableQuery).queryStringLiteral(body);
            output.variables().put(variableName, positionProducer.produce(variableValue));
        }

        return output;
    }
}
