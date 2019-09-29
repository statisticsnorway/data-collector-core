package no.ssb.dc.core.handler;

import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.core.executor.Executor;

import java.util.Map;

@Handler(forClass = Interfaces.Execute.class)
public class ExecuteHandler extends AbstractHandler<Interfaces.Execute> {

    public ExecuteHandler(Interfaces.Execute node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext executeTargetInput = ExecutionContext.of(input);

        // process inputVariable
        for (Map.Entry<String, Interfaces.Query> queryEntry : node.inputVariable().entrySet()) {
            Object itemListItem = input.state(QueryStateHolder.ITEM_LIST_ITEM_DATA);
            Tuple<Position<?>, String> inlineInputItemListItemTuple = Queries.getItemContent(queryEntry.getValue(), itemListItem);
            executeTargetInput.variables().put(queryEntry.getKey(), inlineInputItemListItemTuple.getKey().asString());
        }

        // TODO validate requiredInput

        // execute node
        ExecutionContext output = Executor.execute(node.target(), executeTargetInput);
        return output;
    }
}
