package no.ssb.dc.core.handler;

import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.QueryType;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.core.executor.Executor;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Handler(forClass = Interfaces.NextPage.class)
public class NextPageHandler extends AbstractHandler<Interfaces.NextPage> {

    public NextPageHandler(Interfaces.NextPage node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.empty();

        Response response = input.state(Response.class);
        byte[] body = response.body();

        for (Map.Entry<String, Interfaces.Query> entry : node.outputs().entrySet()) {
            ExecutionContext executionInput = ExecutionContext.of(input);
            executionInput.state(QueryType.class, QueryType.ITEM_STRING);
            executionInput.state(QueryStateHolder.QUERY_DATA, new String(body, StandardCharsets.UTF_8));

            String variableName = entry.getKey();
            Interfaces.Query variableQuery = entry.getValue();

            ExecutionContext executionOutput = Executor.execute(variableQuery, executionInput);
            Tuple<Position<?>, String> queryPositionResultAndSubjectStringLiteral = executionOutput.state(QueryStateHolder.QUERY_RESULT);

            output.variables().put(variableName, queryPositionResultAndSubjectStringLiteral.getKey());
        }

        return output;
    }
}
