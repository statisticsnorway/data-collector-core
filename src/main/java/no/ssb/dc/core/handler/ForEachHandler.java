package no.ssb.dc.core.handler;

import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.ForEach;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.core.executor.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Handler(forClass = ForEach.class)
public class ForEachHandler extends AbstractNodeHandler<ForEach> {

    public static final String ADD_BODY_CONTENT = ParallelHandler.ADD_BODY_CONTENT;
    private static final Logger LOG = LoggerFactory.getLogger(ForEachHandler.class);

    public ForEachHandler(ForEach node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);

        // get current response body
        Response response = input.state(Response.class);
        PageEntryState pageEntryState = input.state(PageEntryState.class);
        byte[] body = pageEntryState == null ? response.body() : pageEntryState.content;

        // evaluate queries for list and item
        DocumentParserFeature parser = Queries.parserFor(node.splitToListQuery().getClass());
        QueryFeature splitQuery = Queries.from(node.splitToListQuery());
        List<JsonNode> list = (List<JsonNode>) splitQuery.evaluateList(body);

        for (JsonNode item : list) {
            byte[] serializedItem = parser.serialize(item);
            PageEntryState nestedEntry = new PageEntryState(item, serializedItem);
            for (Node step : node.steps()) {
                ExecutionContext stepInput = ExecutionContext.of(input).state(PageEntryState.class, nestedEntry).state(ADD_BODY_CONTENT, true);
                Executor.execute(step, stepInput);
            }
        }

        return ExecutionContext.empty();
    }
}
