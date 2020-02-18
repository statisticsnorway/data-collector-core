package no.ssb.dc.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import no.ssb.dc.api.Builders;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.node.JqPath;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.core.handler.Queries;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class JqPathTest {

    String json = "{" +
            "  \"kode\": \"SP-002\"," +
            "  \"melding\": \"identifikator har ugyldig format. Forventet en personidentifikator på 11 siffer\"," +
            "  \"korrelasjonsid\": \"b0e88d88ab83b3cd417d2ee88a696afb\" " +
            "}";

    @Test
    void testJacksonJQ() throws JsonProcessingException {
        ObjectMapper mapper = JsonParser.createJsonParser().mapper();
        Scope rootScope = Scope.newEmptyScope();
        BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_6, rootScope);

        Scope childScope = Scope.newChildScope(rootScope);
        JsonNode in = mapper.readTree(json);
        JsonQuery query = JsonQuery.compile(".kode", Versions.JQ_1_6);
        List<JsonNode> out = new ArrayList<>();
        query.apply(childScope, in, out::add);
        System.out.printf("result: %s", out);
    }

    @Test
    void testJqDocumentParser() {
        DocumentParserFeature parser = Queries.parserFor(JqPath.class);
        ObjectNode serialized = (ObjectNode) parser.deserialize(json.getBytes());
        byte[] deserialized = parser.serialize(serialized);
        assertEquals(serialized.toString(), new String(deserialized));
    }

    @Test
    void testQueryList() {
        // TODO
    }

    @Test
    void testQueryObject() {
        // TODO
    }

    @Test
    void testQueryStringLiteral() {
        JqPath jqPath = Builders.jqpath(".kode").build();
        QueryFeature jq = Queries.from(jqPath);
        String result = jq.evaluateStringLiteral(json);
        assertEquals(result, "SP-002");
    }

    @Test
    void testIllegalQueryStringLiteralReturnNull() {
        JqPath jqPath = Builders.jqpath(".").build();
        QueryFeature jq = Queries.from(jqPath);
        String result = jq.evaluateStringLiteral(json);
        assertNull(result);
    }
}
