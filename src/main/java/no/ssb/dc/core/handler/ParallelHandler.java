package no.ssb.dc.core.handler;

import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Execute;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.Parallel;
import no.ssb.dc.core.executor.Executor;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Handler(forClass = Parallel.class)
public class ParallelHandler extends AbstractHandler<Parallel> {

    public static final String MAX_NUMBER_OF_ITERATIONS = "MAX_NUMBER_OF_ITERATIONS";
    static final String ADD_BODY_CONTENT = "ADD_BODY_CONTENT";

    static final AtomicLong countNumberOfIterations = new AtomicLong(-1);

    public ParallelHandler(Parallel node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        Response response = input.state(Response.class);
        List<?> itemList = Queries.evaluate(node.splitQuery()).queryList(response.body());

        // add correlation-id before fan-out
        CorrelationIds.of(input).add();

        if (input.state(MAX_NUMBER_OF_ITERATIONS) != null) {
            countNumberOfIterations.incrementAndGet();
        }

        for (Object nodeItem : itemList) {

            if (input.state(MAX_NUMBER_OF_ITERATIONS) != null) {
                long maxNumberOfIterations = Long.parseLong(input.state(MAX_NUMBER_OF_ITERATIONS).toString());
                if (ParallelHandler.countNumberOfIterations.get() >= maxNumberOfIterations) {
                    throw new EndOfStreamException();
                }
            }

            byte[] serializedItem = Queries.evaluate(node.splitQuery()).serialize(nodeItem);

            /*
             * Resolve variables
             */
            node.variableNames().stream().forEachOrdered(variableKey -> {
                String value = Queries.evaluate(node.variable(variableKey)).queryStringLiteral(nodeItem);
                input.variable(variableKey, value);
            });

            /*
             * execute step nodes
             */
            ExecutionContext accumulated = ExecutionContext.empty();
            for (Node step : node.steps()) {
                ExecutionContext stepInput = ExecutionContext.of(input).merge(accumulated);
                stepInput.state(PageEntryState.class, new PageEntryState(nodeItem, serializedItem));

                // set state nestedOperation to true to inform AddContentHandler to buffer response body
                if (step instanceof Execute) {
                    stepInput.state(ADD_BODY_CONTENT, true);
                }

                ExecutionContext stepOutput = Executor.execute(step, stepInput);
                accumulated.merge(stepOutput);
            }

            if (ParallelHandler.countNumberOfIterations.get() > -1) {
                ParallelHandler.countNumberOfIterations.incrementAndGet();
            }
        }

        return ExecutionContext.empty().merge(CorrelationIds.of(input).context());
    }

}
