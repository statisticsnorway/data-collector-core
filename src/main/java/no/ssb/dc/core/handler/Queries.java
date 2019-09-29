package no.ssb.dc.core.handler;

import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.QueryType;
import no.ssb.dc.api.handler.Tuple;
import no.ssb.dc.api.node.Query;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.handler.state.QueryStateHolder;

import java.util.List;
import java.util.Map;

public class Queries {

    // Raw payload as byte-array
    public static List<?> getItemList(Query query, ExecutionContext previousInput) {
        ExecutionContext splitInput = ExecutionContext.of(previousInput);
        splitInput.state(QueryType.class, QueryType.ITEM_LIST);
        ExecutionContext splitOutput = Executor.execute(query, splitInput);
        return (List<?>) splitOutput.state(QueryStateHolder.QUERY_RESULT);
    }

    // E.g. List<Document> for Xml for List<JsonNode> for Json. If this breaks Json, the List<?> should be NodeList for XML and ArrayNode for Json
    public static Map<Position<?>, String> getPositionMap(Query query, List<?> itemList) {
        ExecutionContext itemListInput = ExecutionContext.empty();
        itemListInput.state(QueryType.class, QueryType.POSITION_MAP);
        itemListInput.state(QueryStateHolder.QUERY_DATA, itemList);
        ExecutionContext itemListOutput = Executor.execute(query, itemListInput);
        return itemListOutput.state(QueryStateHolder.QUERY_RESULT);
    }

    // E.g. Document for XML or JsonNode for Json
    public static Tuple<Position<?>, String> getItemContent(Query query, Object itemListItem) {
        ExecutionContext itemListItemInput = ExecutionContext.empty();
        itemListItemInput.state(QueryType.class, QueryType.ITEM_LIST_ITEM);
        itemListItemInput.state(QueryStateHolder.QUERY_DATA, itemListItem);
        ExecutionContext itemListItemOutput = Executor.execute(query, itemListItemInput);
        return itemListItemOutput.state(QueryStateHolder.QUERY_RESULT);
    }

    // E.g. String for XML or String for Json
    public static Tuple<Position<?>, String> getItemContent(Query query, String content) {
        ExecutionContext contentInput = ExecutionContext.empty();
        contentInput.state(QueryType.class, QueryType.POSITION_ITEM);
        contentInput.state(QueryStateHolder.QUERY_DATA, content);
        ExecutionContext contentOutput = Executor.execute(query, contentInput);
        return contentOutput.state(QueryStateHolder.QUERY_RESULT);
    }

    // E.g. String for XML or String for Json
    public static Tuple<String, String> getTextContentByNode(Query query, Object content) {
        ExecutionContext contentInput = ExecutionContext.empty();
        contentInput.state(QueryType.class, QueryType.ITEM_NODE_LITERAL);
        contentInput.state(QueryStateHolder.QUERY_DATA, content);
        ExecutionContext contentOutput = Executor.execute(query, contentInput);
        return contentOutput.state(QueryStateHolder.QUERY_RESULT);
    }

    // E.g. String for XML or String for Json
    public static Tuple<String, String> getTextContent(Query query, String content) {
        ExecutionContext contentInput = ExecutionContext.empty();
        contentInput.state(QueryType.class, QueryType.ITEM_STRING);
        contentInput.state(QueryStateHolder.QUERY_DATA, content);
        ExecutionContext contentOutput = Executor.execute(query, contentInput);
        return contentOutput.state(QueryStateHolder.QUERY_RESULT);
    }

    // execute regex
    public static Tuple<Position<?>, String> regex(Query query, ExecutionContext previousInput, String content) {
        ExecutionContext regexInput = ExecutionContext.of(previousInput);
        regexInput.state(QueryType.class, QueryType.ITEM_STRING);
        regexInput.state(QueryStateHolder.QUERY_DATA, content);
        ExecutionContext regexOutput = Executor.execute(query, regexInput);
        return regexOutput.state(QueryStateHolder.QUERY_RESULT);
    }

}
