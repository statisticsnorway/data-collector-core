package no.ssb.dc.core.handler;

import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.ExpressionLanguage;
import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Interfaces;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.executor.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Handler(forClass = Interfaces.Paginate.class)
public class PaginateHandler extends AbstractHandler<Interfaces.Paginate> {

    public static final String CORRELATION_ID = "CORRELATION_ID";
    static final String ADD_PAGE_CONTENT = "ADD_PAGE_CONTENT";
    private final Logger LOG = LoggerFactory.getLogger(PaginateHandler.class);


    public PaginateHandler(Interfaces.Paginate node) {
        super(node);
    }

    /**
     * The Worker manages the pagination lifecycle
     */
    @Override
    public ExecutionContext execute(ExecutionContext input) {

        try {
            Worker worker = new Worker(node, input);
            ExecutionContext output = worker.run();
            return output;

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute targets
     */
    public ExecutionContext executeWork(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.of(input);
        do {

            for (Interfaces.Execute target : node.targets()) {
                ExecutionContext targetInput = ExecutionContext.of(input);

                for (String variableName : node.variableNames()) {
                    ExpressionLanguage el = new ExpressionLanguage(input.variables());
                    String elExpr = node.variable(variableName);
                    if (el.isExpression(elExpr) && input.variables().containsKey(el.getExpression(elExpr))) {
                        Object elValue = el.evaluateExpression(elExpr);
                        targetInput.variables().put(variableName, elValue);
                    }
                }

                targetInput.state(ADD_PAGE_CONTENT, node.addPageContent());

                // add correlation-id on fan-out
                CorrelationIds.of(targetInput).add();

                try {
                    ExecutionContext targetOutput = Executor.execute(target, targetInput);

                    // merge returned variables
                    output.merge(targetOutput);

                    //CorrelationIds.create(input).tail(CorrelationIds.of(targetInput));

                } catch (EndOfStreamException e) {
                    for (String variableName : node.variableNames()) {
                        ExpressionLanguage el = new ExpressionLanguage(input.variables());
                        String elExpr = node.variable(variableName);
                        if (el.isExpression(elExpr)) {
                            output.variables().remove(el.getExpression(elExpr));
                        }
                    }
                    break;
                }
            }

            // forward next page condition
            if (Conditions.untilCondition(node.condition(), output)) {
                Position<?> nextPagePosition = (Position<?>) output.variables().get(node.condition().identifier());
                input.variables().put(node.condition().identifier(), nextPagePosition.value());
            }

        } while (Conditions.untilCondition(node.condition(), output));

        LOG.info("Paginate has completed!");

        return output;
    }

}
