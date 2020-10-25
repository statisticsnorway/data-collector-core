package no.ssb.dc.core.handler;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.SupportHandler;
import no.ssb.dc.api.node.JsonToken;
import no.ssb.dc.api.util.JsonParser;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

@SupportHandler(forClass = JsonToken.class, selectorClass = DocumentParserFeature.class)
public class JsonTokenParser implements DocumentParserFeature {

    private final JsonParser jsonParser;

    public JsonTokenParser() {
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
        try {
            JsonFactory jsonfactory = new JsonFactory();
            try (BufferedInputStream bis = new BufferedInputStream(source)) {
                try (com.fasterxml.jackson.core.JsonParser parser = jsonfactory.createParser(bis)) {
                    // fail if json is not an array
                    if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_ARRAY) {
                        throw new IllegalStateException("Array node NOT found!");
                    }

                    // read array tokens
                    while (parser.nextToken() == com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                        JsonNode jsonNode = jsonParser.mapper().readValue(parser, JsonNode.class);
                        entryCallback.accept(jsonNode);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
