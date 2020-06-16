package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.BodyPublisherProducer;
import no.ssb.dc.api.node.StringBodyPublisherProducer;

@Handler(forClass = StringBodyPublisherProducer.class)
public class StringBodyPublisherProducerHandler extends AbstractHandler<StringBodyPublisherProducer> {

    public StringBodyPublisherProducerHandler(StringBodyPublisherProducer node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        String text = evaluateExpression(context, node.text());
        return ExecutionContext.empty().state(BodyPublisherProducer.class, text.getBytes());
    }

    private String evaluateExpression(ExecutionContext context, String text) {
        ExpressionLanguage el = new ExpressionLanguage(context);
        if (el.isExpression(text)) {
            return el.evaluateExpressions(text);
        } else {
            return text;
        }
    }

}
