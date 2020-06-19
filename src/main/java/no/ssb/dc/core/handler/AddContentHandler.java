package no.ssb.dc.core.handler;

import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.AddContent;

import java.util.LinkedHashMap;
import java.util.Map;

@Handler(forClass = AddContent.class)
public class AddContentHandler extends AbstractNodeHandler<AddContent> {

    public AddContentHandler(AddContent node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        ExpressionLanguage el = new ExpressionLanguage(context);
        String position = (String) el.evaluateExpression(node.positionVariableExpression());

        PageEntryState pageEntryState = context.state(PageEntryState.class);

        ContentStore contentStore = context.services().get(ContentStore.class);

        boolean bufferResponseBody = context.state(ParallelHandler.ADD_BODY_CONTENT) == null ? false : context.state(ParallelHandler.ADD_BODY_CONTENT);

        // evaluate state - single expression for key and multiple expressions for value
        HttpRequestInfo httpRequestInfo = context.state(HttpRequestInfo.class);
        if (!node.state().isEmpty()) {
            Map<String, Object> evaluatedState = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : node.state().entrySet()) {
                String key = entry.getKey();

                if (el.isExpression(key)) {
                    key = (String) el.evaluateExpression(key);
                }

                Object value = entry.getValue();

                if (value instanceof String && el.isExpression((String) value)) {
                    value = el.evaluateExpressions((String) value);
                }

                evaluatedState.put(key, value);
            }
            httpRequestInfo.storeState(evaluatedState);
        }

        String topicName = node.configurations().flowContext().topic();
        if (topicName == null) {
            throw new IllegalStateException("Unable to resolve topic!");
        }

        String contentKey = node.contentKey();
        if (el.isExpression(contentKey)) {
            contentKey = el.evaluateExpressions(contentKey);
        }

        byte[] content = bufferResponseBody ? context.state(Response.class).body() : pageEntryState.content;

        // feature request: decompression 'content' can be handled here

        if (bufferResponseBody) {
            contentStore.bufferDocument(topicName, position, contentKey, content, httpRequestInfo);
        } else {
            contentStore.bufferPaginationEntryDocument(topicName, position, contentKey, content, httpRequestInfo);
        }

        return ExecutionContext.empty();
    }
}
