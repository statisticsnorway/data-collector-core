package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.handler.QueryResult;
import no.ssb.dc.api.handler.QueryState;
import no.ssb.dc.api.node.Base;

import java.util.List;

public abstract class AbstractQueryHandler<N extends Base> extends AbstractHandler<N> implements QueryFeature {

    private ExecutionContext context;

    public AbstractQueryHandler(N node) {
        super(node);
    }

    // TODO enable EL support for all query handlers. Only implemented jq eval literal.
    protected ExecutionContext context() {
        return context;
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        this.context = input;
        QueryState<?> queryState = input.state(QueryState.class);

        if (queryState == null) {
            throw new IllegalStateException("QueryState is NOT set in ExecutionContext.state()");
        }

        if (queryState.type() == Type.LIST) {
            List<?> nodeList = evaluateList(queryState.data());
            return ExecutionContext.empty().state(QueryResult.class, new QueryResult<>(nodeList));

        } else if (queryState.type() == Type.OBJECT) {
            Object node = evaluateObject(queryState.data());
            return ExecutionContext.empty().state(QueryResult.class, new QueryResult<>(node));


        } else if (queryState.type() == Type.STRING_LITERAL) {
            String literal = evaluateStringLiteral(queryState.data());
            return ExecutionContext.empty().state(QueryResult.class, new QueryResult<>(literal));
        }

        return ExecutionContext.empty();
    }
}
