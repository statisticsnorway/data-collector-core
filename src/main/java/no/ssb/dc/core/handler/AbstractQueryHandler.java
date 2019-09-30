package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.handler.QueryResult;
import no.ssb.dc.api.handler.QueryState;
import no.ssb.dc.api.node.Base;

import java.util.List;

public abstract class AbstractQueryHandler<N extends Base> extends AbstractHandler<N> implements QueryFeature {

    public AbstractQueryHandler(N node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        QueryState<?> queryState = input.state(QueryState.class);

        if (queryState == null) {
            throw new IllegalStateException("QueryState is NOT set in ExecutionContext.state()");
        }

        if (queryState.type() == Type.LIST) {
            List<?> nodeList = queryList(queryState.data());
            return ExecutionContext.empty().state(QueryResult.class, new QueryResult<>(nodeList));

        } else if (queryState.type() == Type.OBJECT) {
            Object node = queryObject(queryState.data());
            return ExecutionContext.empty().state(QueryResult.class, new QueryResult<>(node));


        } else if (queryState.type() == Type.STRING_LITERAL) {
            String literal = queryStringLiteral(queryState.data());
            return ExecutionContext.empty().state(QueryResult.class, new QueryResult<>(literal));
        }

        return ExecutionContext.empty();
    }
}
