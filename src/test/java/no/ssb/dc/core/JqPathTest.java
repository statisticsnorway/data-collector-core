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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JqPathTest {

    final String array_json = "{" +
            "  \"items\" : [ {" +
            "    \"id\" : \"672842230\"," +
            "    \"name\" : \"f1.txt\"," +
            "    \"path\" : \"/Home/moveitapi/f1.txt\"," +
            "    \"size\" : 6," +
            "    \"uploadStamp\" : \"2020-05-18T15:33:43\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"672685495\"," +
            "    \"name\" : \"f2.txt\"," +
            "    \"path\" : \"/Home/moveitapi/f2.txt\"," +
            "    \"size\" : 6," +
            "    \"uploadStamp\" : \"2020-05-18T15:33:43\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"672891964\"," +
            "    \"name\" : \"f3.txt\"," +
            "    \"path\" : \"/Home/moveitapi/f3.txt\"," +
            "    \"size\" : 6," +
            "    \"uploadStamp\" : \"2020-05-18T15:33:43\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"672867641\"," +
            "    \"name\" : \"f4.txt\"," +
            "    \"path\" : \"/Home/moveitapi/f4.txt\"," +
            "    \"size\" : 6," +
            "    \"uploadStamp\" : \"2020-05-18T15:33:43\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"672835987\"," +
            "    \"name\" : \"f5.txt\"," +
            "    \"path\" : \"/Home/moveitapi/f5.txt\"," +
            "    \"size\" : 6," +
            "    \"uploadStamp\" : \"2020-05-18T15:33:43\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"674865097\"," +
            "    \"name\" : \"file10.txt\"," +
            "    \"path\" : \"/Home/moveitapi/file10.txt\"," +
            "    \"size\" : 11," +
            "    \"uploadStamp\" : \"2020-05-26T13:13:32\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"674956361\"," +
            "    \"name\" : \"file11.txt\"," +
            "    \"path\" : \"/Home/moveitapi/file11.txt\"," +
            "    \"size\" : 11," +
            "    \"uploadStamp\" : \"2020-05-26T13:13:56\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"674879724\"," +
            "    \"name\" : \"file12.txt\"," +
            "    \"path\" : \"/Home/moveitapi/file12.txt\"," +
            "    \"size\" : 11," +
            "    \"uploadStamp\" : \"2020-05-26T13:43:17\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"675007871\"," +
            "    \"name\" : \"file13.txt\"," +
            "    \"path\" : \"/Home/moveitapi/file13.txt\"," +
            "    \"size\" : 11," +
            "    \"uploadStamp\" : \"2020-05-26T17:14:59\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"674561079\"," +
            "    \"name\" : \"foo1.txt\"," +
            "    \"path\" : \"/Home/moveitapi/foo1.txt\"," +
            "    \"size\" : 5," +
            "    \"uploadStamp\" : \"2020-05-25T14:41:19\"," +
            "    \"isNew\" : false" +
            "  }, {" +
            "    \"id\" : \"674932103\"," +
            "    \"name\" : \"foo3.txt\"," +
            "    \"path\" : \"/Home/moveitapi/foo3.txt\"," +
            "    \"size\" : 5," +
            "    \"uploadStamp\" : \"2020-05-26T13:10:45\"," +
            "    \"isNew\" : false" +
            "  } ]," +
            "  \"paging\" : {" +
            "    \"page\" : 1," +
            "    \"perPage\" : 25," +
            "    \"totalItems\" : 11," +
            "    \"totalPages\" : 1" +
            "  }," +
            "  \"sorting\" : [ {" +
            "    \"sortField\" : \"path\"," +
            "    \"sortDirection\" : \"asc\"" +
            "  } ]" +
            "}";
    String json404withResponseError = "{" +
            "  \"kode\": \"SP-002\"," +
            "  \"melding\": \"identifikator har ugyldig format. Forventet en personidentifikator på 11 siffer\"," +
            "  \"korrelasjonsid\": \"b0e88d88ab83b3cd417d2ee88a696afb\" " +
            "}";
    String json_node = "{" +
            "    \"id\" : \"672842230\"," +
            "    \"name\" : \"f1.txt\"," +
            "    \"path\" : \"/Home/moveitapi/f1.txt\"," +
            "    \"size\" : 6," +
            "    \"uploadStamp\" : \"2020-05-18T15:33:43\"," +
            "    \"isNew\" : false" +
            "  }";

    @Test
    void testJacksonJQ() throws JsonProcessingException {
        ObjectMapper mapper = JsonParser.createJsonParser().mapper();
        Scope rootScope = Scope.newEmptyScope();
        BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_6, rootScope);

        Scope childScope = Scope.newChildScope(rootScope);
        JsonNode in = mapper.readTree(json404withResponseError);
        JsonQuery query = JsonQuery.compile(".kode", Versions.JQ_1_6);
        List<JsonNode> out = new ArrayList<>();
        query.apply(childScope, in, out::add);
        System.out.printf("result: %s", out);
    }

    @Test
    void testJqDocumentParser() {
        DocumentParserFeature parser = Queries.parserFor(JqPath.class);
        ObjectNode serialized = (ObjectNode) parser.deserialize(json404withResponseError.getBytes());
        byte[] deserialized = parser.serialize(serialized);
        assertEquals(serialized.toString(), new String(deserialized));
    }

    @Test
    void testQueryList() {
        JqPath jqPath = Builders.jqpath(".items[]").build();
        QueryFeature jq = Queries.from(jqPath);
        List<?> itemNode = jq.evaluateList(array_json);
        assertFalse(itemNode.isEmpty(), "result is empty");
        assertTrue(itemNode.size() > 1, "result is not >1");
        itemNode.forEach(n -> System.out.printf("%s%n", n));
    }

    @Test
    void testQueryObject() {
        JqPath jqPath = Builders.jqpath(".id").build();
        QueryFeature jq = Queries.from(jqPath);
        JsonNode itemNode = (JsonNode) jq.evaluateObject(json_node);
        assertEquals("672842230", itemNode.textValue());
    }

    @Test
    void testQueryStringLiteral() {
        JqPath jqPath = Builders.jqpath(".kode").build();
        QueryFeature jq = Queries.from(jqPath);
        String result = jq.evaluateStringLiteral(json404withResponseError);
        assertEquals(result, "SP-002");
    }

    @Test
    void testIllegalQueryStringLiteralReturnNull() {
        JqPath jqPath = Builders.jqpath(".").build();
        QueryFeature jq = Queries.from(jqPath);
        String result = jq.evaluateStringLiteral(json404withResponseError);
        assertNull(result);
    }
}
