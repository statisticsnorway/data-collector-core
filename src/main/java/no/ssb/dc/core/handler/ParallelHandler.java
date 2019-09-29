package no.ssb.dc.core.handler;

import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.core.executor.Executor;

import java.util.List;

@Handler(forClass = Interfaces.Parallel.class)
public class ParallelHandler extends AbstractHandler<Interfaces.Parallel> {

    static final String ADD_BODY_CONTENT = "ADD_BODY_CONTENT";

    public ParallelHandler(Interfaces.Parallel node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        List<?> itemList = Queries.getItemList(node.splitQuery(), input);

        // add correlation-id before fan-out
        CorrelationIds.of(input).add();

        for (Object nodeItem : itemList) {

            /*
             * Resolve variables
             */
            node.variableNames().stream().forEachOrdered(variableKey -> {
                Tuple<String, String> tuple = Queries.getTextContentByNode(node.variable(variableKey), nodeItem);
                input.variable(variableKey, tuple);
            });


            /*
             * execute step nodes
             */
            ExecutionContext accumulated = ExecutionContext.empty();
            for (Interfaces.Node step : node.steps()) {
                ExecutionContext stepInput = ExecutionContext.of(input).merge(accumulated);
                stepInput.state(QueryStateHolder.ITEM_LIST_ITEM_DATA, nodeItem);

                // set state nestedOperation to true to inform AddContentHandler to buffer response body
                if (step instanceof Interfaces.Execute) {
                    stepInput.state(ADD_BODY_CONTENT, true);
                }

                ExecutionContext stepOutput = Executor.execute(step, stepInput);
                accumulated.merge(stepOutput);
            }
        }

        return ExecutionContext.empty().merge(CorrelationIds.of(input).context());
    }

}
