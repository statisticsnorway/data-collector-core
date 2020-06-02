package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.handler.QueryResult;
import no.ssb.dc.api.handler.QueryState;
import no.ssb.dc.api.node.Query;
import no.ssb.dc.core.executor.Executor;

import java.util.List;

public class Queries {

    public static DocumentParserFeature parserFor(Class<? extends Query> queryClass) {
        if (!queryClass.isInterface()) {
            queryClass = (Class<? extends Query>) queryClass.getInterfaces()[0];
        }
        DocumentParserFeature parser = Handlers.createSupportHandlerFor(queryClass, DocumentParserFeature.class);
        return new DocumentParserWrapper(parser);
    }

    public static QueryFeature from(Query query) {
        return new QueryHandlerWrapper(query);
    }

    public static QueryFeature from(ExecutionContext context, Query query) {
        return new QueryHandlerWrapper(context, query);
    }

    static class DocumentParserWrapper implements DocumentParserFeature {

        private final DocumentParserFeature parser;

        DocumentParserWrapper(DocumentParserFeature parser) {
            this.parser = parser;
        }

        @Override
        public byte[] serialize(Object document) {
            return parser.serialize(document);
        }

        @Override
        public Object deserialize(byte[] source) {
            return parser.deserialize(source);
        }
    }

    static class QueryHandlerWrapper implements QueryFeature {

        private final ExecutionContext context;
        private final Query query;

        QueryHandlerWrapper(Query query) {
            this.context = ExecutionContext.empty();
            this.query = query;
        }

        QueryHandlerWrapper(ExecutionContext context, Query query) {
            this.context = ExecutionContext.of(context);
            this.query = query;
        }

        @Override
        public List<?> evaluateList(Object data) {
            context.state(QueryState.class, new QueryState<>(QueryFeature.Type.LIST, data));
            ExecutionContext output = Executor.execute(query, context);
            QueryResult<List<?>> state = output.state(QueryResult.class);
            return state.getResult();
        }

        @Override
        public Object evaluateObject(Object data) {
            context.state(QueryState.class, new QueryState<>(Type.OBJECT, data));
            ExecutionContext output = Executor.execute(query, context);
            QueryResult<Object> state = output.state(QueryResult.class);
            return state.getResult();
        }

        @Override
        public String evaluateStringLiteral(Object data) {
            context.state(QueryState.class, new QueryState<>(Type.STRING_LITERAL, data));
            ExecutionContext output = Executor.execute(query, context);
            QueryResult<String> state = output.state(QueryResult.class);
            return state.getResult();
        }
    }
}
