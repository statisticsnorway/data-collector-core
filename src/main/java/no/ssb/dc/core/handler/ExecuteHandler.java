package no.ssb.dc.core.handler;

import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Tuple;
import no.ssb.dc.api.node.Execute;
import no.ssb.dc.api.node.Query;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.handler.state.QueryStateHolder;

import java.util.Map;

@Handler(forClass = Execute.class)
public class ExecuteHandler extends AbstractHandler<Execute> {

    public ExecuteHandler(Execute node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext executeTargetInput = ExecutionContext.of(input);

        // process inputVariable
        for (Map.Entry<String, Query> queryEntry : node.inputVariable().entrySet()) {
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
