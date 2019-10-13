package no.ssb.dc.core.handler;

import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.NextPage;
import no.ssb.dc.api.node.Query;

import java.util.Map;

@Handler(forClass = NextPage.class)
public class NextPageHandler extends AbstractNodeHandler<NextPage> {

    public NextPageHandler(NextPage node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        ExecutionContext output = ExecutionContext.empty();

        Response response = input.state(Response.class);
        byte[] body = response.body();

        PageContext.Builder pageContextBuilder = input.state(PageContext.Builder.class);

        for (Map.Entry<String, Query> entry : node.outputs().entrySet()) {
            String variableName = entry.getKey();
            Query variableQuery = entry.getValue();
            String variableValue = Queries.from(variableQuery).evaluateStringLiteral(body);
            output.variables().put(variableName, variableValue);
            pageContextBuilder.addNextPosition(variableName, variableValue);
        }

        return output;
    }
}
