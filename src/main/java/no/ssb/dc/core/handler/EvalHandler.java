package no.ssb.dc.core.handler;

import no.ssb.dc.api.ExpressionLanguage;
import no.ssb.dc.api.Handler;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.delegate.QueryType;
import no.ssb.dc.api.delegate.Tuple;
import no.ssb.dc.api.node.Eval;
import no.ssb.dc.core.executor.Executor;

@Handler(forClass = Eval.class)
public class EvalHandler extends AbstractHandler<Eval> {

    public EvalHandler(Eval node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExecutionContext output = ExecutionContext.of(input);

        ExecutionContext queryInput = ExecutionContext.of(input);
        queryInput.state(QueryType.class, input.state(QueryType.class));
        queryInput.state(QueryStateHolder.QUERY_DATA, input.state(QueryStateHolder.QUERY_DATA));
        ExecutionContext queryOutput = Executor.execute(node.query(), queryInput);
        Tuple<String, String> queryTuple = queryOutput.state(QueryStateHolder.QUERY_RESULT);

        String queryValue = queryTuple.getKey();
        ExecutionContext evalContext = ExecutionContext.of(queryInput);
        evalContext.variable(node.bind(), queryValue);

        ExpressionLanguage el = new ExpressionLanguage(evalContext.variables());
        Object evalResult = el.evaluateExpression(node.expression());
        Tuple<Position<?>, String> evalTuple = new Tuple<>(new Position<>(evalResult), queryInput.state(QueryStateHolder.QUERY_DATA));
        output.state(QueryStateHolder.QUERY_RESULT, evalTuple);

        return output;
    }
}
