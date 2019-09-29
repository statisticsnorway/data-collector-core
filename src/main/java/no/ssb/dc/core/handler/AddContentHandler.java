package no.ssb.dc.core.handler;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.ExpressionLanguage;
import no.ssb.dc.api.Handler;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.AddContent;

@Handler(forClass = AddContent.class)
public class AddContentHandler extends AbstractHandler<AddContent> {

    public AddContentHandler(AddContent node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        ExpressionLanguage el = new ExpressionLanguage(context.variables());
        Tuple<String, String> positionAndEntryData = (Tuple<String, String>) el.evaluateExpression(node.positionVariableExpression());

        ConfigurationMap config = context.services().get(ConfigurationMap.class);
        ContentStore contentStore = context.services().get(ContentStore.class);


        boolean bufferResponseBody = context.state(ParallelHandler.ADD_BODY_CONTENT) == null ? false : context.state(ParallelHandler.ADD_BODY_CONTENT);

        HttpRequestInfo httpRequestInfo = context.state(HttpRequestInfo.class);

        if (bufferResponseBody) {
            Response response = context.state(Response.class);
            contentStore.bufferDocument(config.get("namespace.default"), positionAndEntryData.getKey(), node.contentKey(), response.body(), httpRequestInfo);
        } else {
            contentStore.bufferPaginationEntryDocument(config.get("namespace.default"), positionAndEntryData.getKey(), node.contentKey(), positionAndEntryData.getValue().getBytes(), httpRequestInfo);
        }
        return ExecutionContext.empty();
    }
}
