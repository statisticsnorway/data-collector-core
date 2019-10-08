package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.handler.QueryState;
import no.ssb.dc.api.node.Eval;

import java.util.List;

@Handler(forClass = Eval.class)
public class EvalHandler extends AbstractQueryHandler<Eval> {

    public EvalHandler(Eval node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        QueryState queryState = input.state(QueryState.class);

        if (queryState == null) {
            throw new IllegalArgumentException("QueryState is not set!");
        }

        if (queryState.type() != Type.STRING_LITERAL) {
            throw new RuntimeException("Only QueryFeature.Type.STRING_LITERAL is supported!");
        }

        /*
         * execute sub-query and bind variable to output
         */

        String result = Queries.from(node.query()).evaluateStringLiteral(queryState.data());
        input.variable(node.bind(), result);
//        ExecutionContext output = ExecutionContext.empty();

        /*
         * execute this handler
         */

        ExecutionContext evalContext = ExecutionContext.of(input);
        ExpressionLanguage el = new ExpressionLanguage(input.variables());
        evalContext.state(QueryState.class, new QueryState<>(queryState.type(), el));

//        return output.merge(super.execute(evalContext));
        return super.execute(evalContext);
    }

    @Override
    public byte[] serialize(Object node) {
        throw new UnsupportedOperationException("Serialization is not supported!");
    }

    @Override
    public Object deserialize(byte[] source) {
        throw new UnsupportedOperationException("Deserialization is not supported!");
    }

    @Override
    public List<?> evaluateList(Object data) {
        throw new UnsupportedOperationException("queryList is not supported!");
    }

    @Override
    public Object evaluateObject(Object data) {
        throw new UnsupportedOperationException("queryObject is not supported!");
    }

    @Override
    public String evaluateStringLiteral(Object data) {
        ExpressionLanguage el = (ExpressionLanguage) data;
        Object value = el.evaluateExpression(node.expression());
        return (value != null ? value.toString() : null);
    }
}
