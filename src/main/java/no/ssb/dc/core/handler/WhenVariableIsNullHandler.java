package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.WhenVariableIsNull;
import no.ssb.dc.core.handler.state.ConditionType;

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

        final String variableName = node.identifier();
        final boolean variableExists = input.variables().containsKey(variableName);
        final Object variableValue = input.variables().get(variableName);

        boolean isNull = !variableExists || variableValue == null;
        output.state(ConditionType.UNTIL_CONDITION_RESULT, isNull);
        return output;
    }
}
