package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.Execute;
import no.ssb.dc.api.node.Query;
import no.ssb.dc.core.executor.Executor;

import java.util.Map;

@Handler(forClass = Execute.class)
public class ExecuteHandler extends AbstractNodeHandler<Execute> {

    public ExecuteHandler(Execute node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext executeTargetInput = ExecutionContext.of(super.execute(input));

        // process inputVariable
        for (Map.Entry<String, Query> inlineVariableEntry : node.inputVariable().entrySet()) {
            String inputVariableName = inlineVariableEntry.getKey();
            Query inputVariableQuery = inlineVariableEntry.getValue();

            PageEntryState itemListItem = input.state(PageEntryState.class);
            String inputVariableValue = Queries.from(inputVariableQuery).evaluateStringLiteral(itemListItem.nodeObject);

            executeTargetInput.variables().put(inputVariableName, inputVariableValue);
        }

        // TODO validate requiredInput

        // execute node
        ExecutionContext output = Executor.execute(node.target(), executeTargetInput);
        return output;
    }
}
