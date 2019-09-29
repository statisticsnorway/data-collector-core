package no.ssb.dc.core;

import no.ssb.dc.api.Builders;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Tuple;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.core.handler.Queries;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerListener;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

@Listeners(TestServerListener.class)
public class MockDataTest {

    @Inject TestServer server;

    Response get(String url) {
        return Client.newClient().send(Request.newRequestBuilder()
                .url(server.testURL(url))
                .header("Accept", "application/xml")
                .GET()
                .build());
    }

    @Test
    public void thatAcceptXmlContent() {
        ExecutionContext nodeListInput = ExecutionContext.empty();
        Response response = get("/ns/mock?cursor=1&size=10");
        nodeListInput.state(Response.class, response);

        List<?> itemList = Queries.getItemList(Builders.xpath("/feed/entry").build(), nodeListInput);
        Map<Position<?>, String> expectedPositionsMap = Queries.getPositionMap(Builders.xpath("/entry/id").build(), itemList);

        for (Map.Entry<Position<?>, String> entry : expectedPositionsMap.entrySet()) {
            System.out.printf("Expected-position:\t%s: \t\t\t\t\t\t\t\t\t\t\t%s%n", entry.getKey().value(), entry.getValue());

            Tuple<Position<?>, String> eventPosition = Queries.getItemContent(Builders.xpath("/entry/event/event-id").build(), entry.getValue());

            Response eventResponse = get("/ns/mock/" + eventPosition.getKey().asString() + "?type=event");
            System.out.printf("Event-position: \t%s:  \t\t%s%n", eventPosition.getKey().value(), new String(eventResponse.body()));
        }
    }
}
