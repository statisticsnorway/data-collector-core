package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.error.ExecutionException;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
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

            // PageEntry is propagated by ParallelHandler; used to resolve Entry document. Or else, fallback to response content
            Object content = null;
            PageEntryState itemListItem = input.state(PageEntryState.class);
            Response response = input.state(Response.class);
            if (itemListItem != null) {
                content = itemListItem.nodeObject;

            } else if (response != null) {
                content = input.state(Response.class).body();
            }

            if (content != null) {
                String inputVariableValue = Queries.from(inputVariableQuery).evaluateStringLiteral(content);
                executeTargetInput.variables().put(inputVariableName, inputVariableValue);
            }
        }

        // validate required input variables
        for (String requiredInput : node.requiredInputs()) {
            if (!executeTargetInput.variables().containsKey(requiredInput)) {
                throw new ExecutionException(String.format("Required input variable: '%s' NOT found!", requiredInput));
            }
        }

        // execute node
        ExecutionContext output = Executor.execute(node.target(), executeTargetInput);
        return output;
    }
}
