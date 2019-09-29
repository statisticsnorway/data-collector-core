package no.ssb.dc.core.handler;

import no.ssb.dc.api.Handler;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.WhenVariableIsNull;

@Handler(forClass = WhenVariableIsNull.class)
public class WhenVariableIsNullHandler extends AbstractHandler<WhenVariableIsNull> {

    private final WhenVariableIsNull node;

    public WhenVariableIsNullHandler(WhenVariableIsNull node) {
        super(node);
        this.node = node;
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.empty();
        boolean test = input.variables().containsKey(node.identifier()) || input.variables().get(node.identifier()) != null;
        output.state(ConditionType.UNTIL_CONDITION_RESULT, test);
        return output;
    }
}
