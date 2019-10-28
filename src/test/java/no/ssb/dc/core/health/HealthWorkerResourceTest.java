package no.ssb.dc.core.health;

import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dc.api.content.HealthContentStreamMonitor;
import no.ssb.dc.api.node.Paginate;
import no.ssb.dc.api.util.JsonParser;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

public class HealthWorkerResourceTest {

    @Test
    public void testSerialize() {
        HealthWorkerResource workerResource = new HealthWorkerResource(UUID.randomUUID());
        HealthWorkerMonitor workerMonitor = workerResource.getMonitor();

        workerMonitor.setName("test spec");
        workerMonitor.setStartFunction(Paginate.class.getName());
        workerMonitor.setStartFunctionId("loop");

        workerMonitor.setThreadPoolInfo(Map.of("k1", "v1"));

        workerMonitor.security().setSslBundleName("test-certs");

        for (int i = 0; i < 200; i++) {
            workerMonitor.request().incrementCompletedRequestCount();
            workerMonitor.request().addRequestDurationMillisSeconds(300 * 100);
        }
        workerMonitor.request().incrementRequestRetryOnFailureCount();

        workerMonitor.request().incrementPrefetchCount();
        workerMonitor.request().updateTotalExpectedCount(250);
        workerMonitor.request().updateTotalCompletedCount(200);


        workerMonitor.contentStream().setTopic("topic-1");
        workerMonitor.contentStream().setLastPosition("200");
        workerMonitor.contentStream().setMonitor(new HealthContentStreamMonitor(() -> true));

        workerMonitor.contentStream().monitor().updateLastSeen();

        workerMonitor.contentStream().monitor().incrementPaginationDocumentCount();
        workerMonitor.contentStream().monitor().addPaginationDocumentSize(1200);

        workerMonitor.contentStream().monitor().incrementEntryBufferCount();
        workerMonitor.contentStream().monitor().addEntryBufferSize(100);

        workerMonitor.contentStream().monitor().incrementDocumentBufferCount();
        workerMonitor.contentStream().monitor().addDocumentBufferSize(250);

        workerMonitor.contentStream().monitor().addPublishedBufferCount(2);
        workerMonitor.contentStream().monitor().addPublishedPositionCount(1);

        Object resource = workerResource.resource();

        JsonParser jsonParser = JsonParser.createJsonParser();
        ObjectNode rootNode = jsonParser.createObjectNode();
        ObjectNode convertedNode = jsonParser.mapper().convertValue(resource, ObjectNode.class);
        rootNode.set(workerResource.name(), convertedNode);
        String json = jsonParser.toPrettyJSON(rootNode);

        System.out.printf("%s%n", json);
    }
}
