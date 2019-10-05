package no.ssb.dc.core.handler;

import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.Execute;
import no.ssb.dc.api.node.Paginate;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.executor.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Handler(forClass = Paginate.class)
public class PaginateHandler extends AbstractNodeHandler<Paginate> {

    static final String ADD_PAGE_CONTENT = "ADD_PAGE_CONTENT";
    private final Logger LOG = LoggerFactory.getLogger(PaginateHandler.class);


    public PaginateHandler(Paginate node) {
        super(node);
    }

    public Paginate node() {
        return node;
    }

    /**
     * The Worker manages the pagination lifecycle
     */
    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);

        Lifecycle lifecycle = new Lifecycle(this);
        return lifecycle.execute(input);
    }

    /**
     * Execute targets
     */
    public ExecutionContext doPage(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.of(input);

        for (Execute target : node.targets()) {
            ExecutionContext targetInput = ExecutionContext.of(input);

            // merge input variables with node variables
            node.variableNames().forEach(name -> {
                if (targetInput.variable(name) == null) {
                    targetInput.variable(name, node.variable(name));
                }
            });

            // evaluate expression given that there is an identifier that matches
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

                // TODO fix keep the previous page correlation-id reference
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

        return output;
    }

}
