package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.handler.QueryResult;
import no.ssb.dc.api.handler.QueryState;
import no.ssb.dc.api.node.Query;
import no.ssb.dc.core.executor.Executor;

import java.util.List;

public class Queries {

    public static QueryFeature from(Query query) {
        return new QueryHandlerWrapper(query);
    }

    static class QueryHandlerWrapper implements QueryFeature {

        private final Query query;

        public QueryHandlerWrapper(Query query) {
            this.query = query;
        }

        @Override
        public byte[] serialize(Object node) {
            QueryFeature queryFeature = (QueryFeature) Handlers.createHandlerFor(query);
            return queryFeature.serialize(node);
        }

        @Override
        public Object deserialize(byte[] source) {
            QueryFeature queryFeature = (QueryFeature) Handlers.createHandlerFor(query);
            return queryFeature.deserialize(source);
        }

        @Override
        public List<?> evaluateList(Object data) {
            ExecutionContext input = ExecutionContext.empty();
            input.state(QueryState.class, new QueryState<>(QueryFeature.Type.LIST, data));
            ExecutionContext output = Executor.execute(query, input);
            QueryResult<List<?>> state = output.state(QueryResult.class);
            return state.getResult();
        }

        @Override
        public Object evaluateObject(Object data) {
            ExecutionContext input = ExecutionContext.empty();
            input.state(QueryState.class, new QueryState<>(Type.OBJECT, data));
            ExecutionContext output = Executor.execute(query, input);
            QueryResult<Object> state = output.state(QueryResult.class);
            return state.getResult();
        }

        @Override
        public String evaluateStringLiteral(Object data) {
            ExecutionContext input = ExecutionContext.empty();
            input.state(QueryState.class, new QueryState<>(Type.STRING_LITERAL, data));
            ExecutionContext output = Executor.execute(query, input);
            QueryResult<String> state = output.state(QueryResult.class);
            return state.getResult();
        }
    }
}
