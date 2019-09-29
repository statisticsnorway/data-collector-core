package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Condition;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.handler.state.ConditionType;

public class Conditions {

    public static boolean untilCondition(Condition condition, ExecutionContext context) {
        ExecutionContext untilConditionOutput = Executor.execute(condition, context);
        return untilConditionOutput.state(ConditionType.UNTIL_CONDITION_RESULT);
    }
}
