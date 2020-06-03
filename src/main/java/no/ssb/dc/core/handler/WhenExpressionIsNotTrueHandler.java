package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.WhenExpressionIsTrue;
import no.ssb.dc.core.handler.state.ConditionType;

@Handler(forClass = WhenExpressionIsTrue.class)
public class WhenExpressionIsNotTrueHandler extends AbstractHandler<WhenExpressionIsTrue> {

    private final WhenExpressionIsTrue node;

    public WhenExpressionIsNotTrueHandler(WhenExpressionIsTrue node) {
        super(node);
        this.node = node;
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.empty();

        final String expression = node.identifier();

        ExpressionLanguage el = new ExpressionLanguage(input);
        if (!el.isExpression(expression)) {
            throw new IllegalStateException("Not a valid expression: " + expression);
        }
        boolean isTrue = (boolean) el.evaluateExpression(expression);

        output.state(ConditionType.UNTIL_CONDITION_RESULT, isTrue);
        return output;
    }
}
