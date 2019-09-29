package no.ssb.dc.core.handler;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.ExpressionLanguage;
import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.api.http.Metadata;
import no.ssb.dc.api.http.Response;

@Handler(forClass = Interfaces.AddContent.class)
public class AddContentHandler extends AbstractHandler<Interfaces.AddContent> {

    public AddContentHandler(Interfaces.AddContent node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        ExpressionLanguage el = new ExpressionLanguage(context.variables());
        Tuple<String, String> positionAndEntryData = (Tuple<String, String>) el.evaluateExpression(node.positionVariableExpression());

        ConfigurationMap config = context.services().get(ConfigurationMap.class);
        ContentStore contentStore = context.services().get(ContentStore.class);


        boolean bufferResponseBody = context.state(ParallelHandler.ADD_BODY_CONTENT) == null ? false : context.state(ParallelHandler.ADD_BODY_CONTENT);

        Metadata metadata = context.state(Metadata.class);

        if (bufferResponseBody) {
            Response response = context.state(Response.class);
            contentStore.bufferDocument(config.get("namespace.default"), positionAndEntryData.getKey(), node.contentKey(), response.body(), metadata);
        } else {
            contentStore.bufferPaginationEntryDocument(config.get("namespace.default"), positionAndEntryData.getKey(), node.contentKey(), positionAndEntryData.getValue().getBytes(), metadata);
        }
        return ExecutionContext.empty();
    }
}
