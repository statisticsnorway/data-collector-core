package no.ssb.dc.core.handler;

import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.AddContent;

@Handler(forClass = AddContent.class)
public class AddContentHandler extends AbstractNodeHandler<AddContent> {

    public AddContentHandler(AddContent node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        ExpressionLanguage el = new ExpressionLanguage(context.variables());
        String position = (String) el.evaluateExpression(node.positionVariableExpression());

        PageEntryState pageEntryState = context.state(PageEntryState.class);

        ContentStore contentStore = context.services().get(ContentStore.class);

        boolean bufferResponseBody = context.state(ParallelHandler.ADD_BODY_CONTENT) == null ? false : context.state(ParallelHandler.ADD_BODY_CONTENT);

        HttpRequestInfo httpRequestInfo = context.state(HttpRequestInfo.class);

        String topicName = node.configurations().flowContext().topic();
        if (topicName == null) {
            throw new IllegalStateException("Unable to resolve topic!");
        }

        if (bufferResponseBody) {
            Response response = context.state(Response.class);
            contentStore.bufferDocument(topicName, position, node.contentKey(), response.body(), httpRequestInfo);
        } else {
            contentStore.bufferPaginationEntryDocument(topicName, position, node.contentKey(), pageEntryState.content, httpRequestInfo);
        }
        return ExecutionContext.empty();
    }
}
