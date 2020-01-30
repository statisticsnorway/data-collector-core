package no.ssb.dc.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import no.ssb.dc.api.util.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class JsonPathTest {

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
}
