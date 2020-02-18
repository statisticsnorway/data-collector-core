package no.ssb.dc.core.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.test.client.ResponseHelper;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(TestServerExtension.class)
public class MockDataControllerTest {

    private static final Logger LOG = LoggerFactory.getLogger(MockDataControllerTest.class);

    @Inject
    TestClient client;

    @Test
    public void testMockCursor() {
        String cursor = "1";
        int size = 100;
        ResponseHelper<String> eventsResponse = client.get(String.format("/mock?cursor=%s&size=%s", cursor, size)).expect200Ok();
        ArrayNode arrayNode = JsonParser.createJsonParser().fromJson(eventsResponse.body(), ArrayNode.class);
        assertEquals(arrayNode.iterator().next().get("id").asText(), cursor);
        assertEquals(arrayNode.size(), size);
    }

    @Test
    public void testMockItems() {
        ResponseHelper<String> eventsResponse = client.get("/mock/5").expect200Ok();
        ObjectNode objectNode = JsonParser.createJsonParser().fromJson(eventsResponse.body(), ObjectNode.class);
        assertEquals(objectNode.get("id").asText(), "5");
    }
}
