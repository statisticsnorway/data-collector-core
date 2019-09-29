package no.ssb.dc.core.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dc.api.util.JacksonFactory;
import no.ssb.dc.test.client.ResponseHelper;
import no.ssb.dc.test.client.TestClient;
import no.ssb.dc.test.server.TestServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;

@Listeners(TestServerListener.class)
public class MockDataControllerTest {

    private static final Logger LOG = LoggerFactory.getLogger(MockDataControllerTest.class);

    @Inject
    TestClient client;

    @Test
    public void testMockCursor() {
        String cursor = "1";
        int size = 100;
        ResponseHelper<String> eventsResponse = client.get(String.format("/ns/mock?cursor=%s&size=%s", cursor, size)).expect200Ok();
        ArrayNode arrayNode = JacksonFactory.instance().fromJson(eventsResponse.body(), ArrayNode.class);
        assertEquals(arrayNode.iterator().next().get("id").asText(), cursor);
        assertEquals(arrayNode.size(), size);
    }

    @Test
    public void testMockItems() {
        ResponseHelper<String> eventsResponse = client.get("/ns/mock/5").expect200Ok();
        ObjectNode objectNode = JacksonFactory.instance().fromJson(eventsResponse.body(), ObjectNode.class);
        assertEquals(objectNode.get("id").asText(), "5");
    }
}
