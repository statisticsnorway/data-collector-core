package no.ssb.dc.core.handler;

import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.context.ExecutionContext;

@Handler(forClass = Interfaces.WhenVariableIsNull.class)
public class WhenVariableIsNullHandler extends AbstractHandler<Interfaces.WhenVariableIsNull> {

    private final Interfaces.WhenVariableIsNull node;

    public WhenVariableIsNullHandler(Interfaces.WhenVariableIsNull node) {
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
