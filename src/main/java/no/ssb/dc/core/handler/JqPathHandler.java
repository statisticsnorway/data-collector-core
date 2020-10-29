package no.ssb.dc.core.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.Versions;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.JqPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Handler(forClass = JqPath.class)
public class JqPathHandler extends AbstractQueryHandler<JqPath> {

    private static final Logger LOG = LoggerFactory.getLogger(JqPathHandler.class);

    private static final Version JQ_VERSION = Versions.JQ_1_6;
    private final DocumentParserFeature jsonParser;
    private static final Scope rootScope = Scope.newEmptyScope();

    static {
        BuiltinFunctionLoader.getInstance().loadFunctions(JQ_VERSION, rootScope);
    }

    public JqPathHandler(JqPath node) {
        super(node);
        jsonParser = Queries.parserFor(node.getClass());
    }

    ObjectNode asDocument(Object data) {
        if (data instanceof ObjectNode) {
            return (ObjectNode) data;

        } else if (data instanceof byte[]) {
            return (ObjectNode) jsonParser.deserialize((byte[]) data);

        } else if (data instanceof String) {
            return (ObjectNode) jsonParser.deserialize(((String) data).getBytes());

        } else {
            throw new IllegalArgumentException(String.format("Param value not supported [%s]: %s", data.getClass().getSimpleName(), data));
        }
    }

    @Override
    public List<?> evaluateList(Object data) {
        try {
            ObjectNode jsonNode = asDocument(data);
            if (jsonNode.isEmpty()) {
                return Collections.emptyList();
            }
            Scope childScope = Scope.newChildScope(rootScope);
            JsonQuery query = JsonQuery.compile(evaluateExpression(node.expression()), JQ_VERSION);
            List<JsonNode> result = new ArrayList<>();
            query.apply(childScope, jsonNode, result::add);
            return result;
        } catch (JsonQueryException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object evaluateObject(Object data) {
        try {
            ObjectNode jsonNode = asDocument(data);
            Scope childScope = Scope.newChildScope(rootScope);
            JsonQuery query = JsonQuery.compile(evaluateExpression(node.expression()), JQ_VERSION);
            AtomicReference<JsonNode> outputRef = new AtomicReference<>();
            query.apply(childScope, jsonNode, outputRef::set);
            JsonNode output = outputRef.get();
            /*
            boolean isJsonValue = List.of(JsonNodeType.OBJECT, JsonNodeType.POJO, JsonNodeType.ARRAY).stream().noneMatch(type -> output.getNodeType() == type);
            LOG.trace("isJson: {}", isJsonValue);
            if (isJsonValue) {
                ObjectNode objectNode = JsonParser.createJsonParser().createObjectNode();
                return objectNode.set("value", output);
            }
             */
            return output;
        } catch (JsonQueryException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String evaluateStringLiteral(Object data) {
        try {
            JsonNode jsonNode = (data instanceof JsonNode) ? (JsonNode) data : asDocument(data);
            Scope childScope = Scope.newChildScope(rootScope);
            JsonQuery query = JsonQuery.compile(evaluateExpression(node.expression()), JQ_VERSION);
            List<JsonNode> result = new ArrayList<>();
            query.apply(childScope, jsonNode, result::add);
            if (!result.isEmpty()) {
                JsonNode firstNode = result.get(0);
                switch (firstNode.getNodeType()) {
                    case NUMBER:
                        return firstNode.asText();

                    case STRING:
                        return firstNode.asText();

                    case NULL:
                        return null;

                    case ARRAY:
                    case BINARY:
                    case BOOLEAN:
                    case MISSING:
                    case OBJECT:
                    case POJO:
                    default: {
                        LOG.error("Expression: '{}' caused failure. Node-type {} is not valid for string literal => {}", node.expression(), firstNode.getNodeType(), firstNode);
                        return null;
                    }
                }
            }
            return null;

        } catch (JsonQueryException e) {
            throw new RuntimeException(e);
        }
    }
}
