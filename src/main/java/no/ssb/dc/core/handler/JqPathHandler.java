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
import java.util.List;

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

    JsonNode asDocument(Object data) {
        if (data instanceof ObjectNode) {
            return (ObjectNode) data;

        } else if (data instanceof byte[]) {
            return (JsonNode) jsonParser.deserialize((byte[]) data);

        } else if (data instanceof String) {
            return (JsonNode) jsonParser.deserialize(((String) data).getBytes());

        } else {
            throw new IllegalArgumentException("Param value not supported: " + data);
        }
    }

    @Override
    public List<?> evaluateList(Object data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object evaluateObject(Object data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String evaluateStringLiteral(Object data) {
        try {
            JsonNode jsonNode = asDocument(data);
            Scope childScope = Scope.newChildScope(rootScope);
            JsonQuery query = JsonQuery.compile(node.expression(), JQ_VERSION);
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
