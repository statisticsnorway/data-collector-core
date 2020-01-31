package no.ssb.dc.core.handler;

import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.SupportHandler;
import no.ssb.dc.api.node.JqPath;
import no.ssb.dc.api.util.JsonParser;

import java.io.IOException;

@SupportHandler(forClass = JqPath.class, selectorClass = DocumentParserFeature.class)
public class JqPathParser implements DocumentParserFeature {

    private final JsonParser jsonParser;

    public JqPathParser() {
        jsonParser = JsonParser.createJsonParser();

    }

    @Override
    public byte[] serialize(Object document) {
        return jsonParser.toJSON(document).getBytes();
    }

    @Override
    public Object deserialize(byte[] source) {
        try {
            return jsonParser.mapper().readTree(source);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
