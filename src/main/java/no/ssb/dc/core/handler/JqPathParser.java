package no.ssb.dc.core.handler;

import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.SupportHandler;
import no.ssb.dc.api.node.JqPath;
import no.ssb.dc.api.util.JsonParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

@SupportHandler(forClass = JqPath.class, selectorClass = DocumentParserFeature.class)
public class JqPathParser implements DocumentParserFeature {

    private final JsonParser jsonParser;

    public JqPathParser() {
        jsonParser = JsonParser.createJsonParser();
    }

    /**
     * @param document ObjectNode (JsonNode or ArrayNode)
     * @return byte array
     */
    @Override
    public byte[] serialize(Object document) {
        return jsonParser.toJSON(document).getBytes();
    }

    /**
     * @param source json byte array
     * @return JsonNode
     */
    @Override
    public Object deserialize(byte[] source) {
        try {
            return jsonParser.mapper().readTree(source);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tokenDeserializer(InputStream source, Consumer<Object> entryCallback) {
        throw new UnsupportedOperationException();
    }
}
