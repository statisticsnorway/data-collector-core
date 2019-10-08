package no.ssb.dc.core;

import no.ssb.dc.api.Builders;
import no.ssb.dc.api.Position;
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
import java.util.stream.Collectors;

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
        Response response = get("/ns/mock?cursor=1&size=10");

        List<?> itemList = Queries.from(Builders.xpath("/feed/entry").build()).evaluateList(response.body());

        Map<Position<?>, String> expectedPositionsMap = itemList.stream()
                .map(item -> {
                    Position<?> pos = new Position<>(Queries.from((Builders.xpath("/entry/id").build())).evaluateStringLiteral(item));
                    return new Tuple<>(pos, new String(Queries.from(Builders.xpath("").build()).serialize(item)));
                })
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

        for (Map.Entry<Position<?>, String> entry : expectedPositionsMap.entrySet()) {
            System.out.printf("Expected-position:\t%s: \t\t\t\t\t\t\t\t\t\t\t%s%n", entry.getKey().value(), entry.getValue());

            String eventPosition = Queries.from(Builders.xpath("/entry/event/event-id").build()).evaluateStringLiteral(entry.getValue().getBytes());

            Response eventResponse = get("/ns/mock/" + eventPosition + "?type=event");
            System.out.printf("Event-position: \t%s:  \t\t%s%n", eventPosition, new String(eventResponse.body()));
        }
    }
}
