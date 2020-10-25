package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.handler.QueryResult;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.JsonToken;

import java.nio.charset.StandardCharsets;

@Handler(forClass = JsonToken.class)
public class JsonTokenHandler extends AbstractHandler<JsonToken> {

    private final DocumentParserFeature jsonParser;

    public JsonTokenHandler(JsonToken node) {
        super(node);
        jsonParser = Queries.parserFor(node.getClass());
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        Response response = context.state(Response.class);

        // token based deserializer

        String body = new String(response.body(), StandardCharsets.UTF_8);
        QueryResult<String> queryResult = new QueryResult<>(body);
        ExecutionContext output = ExecutionContext.empty().state(QueryResult.class, queryResult);
        return output;
    }


}
