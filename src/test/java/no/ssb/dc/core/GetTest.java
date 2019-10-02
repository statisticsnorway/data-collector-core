package no.ssb.dc.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.Flow;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.PositionProducer;
import no.ssb.dc.api.Processor;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Get;
import no.ssb.dc.api.node.builder.FlowBuilder;
import no.ssb.dc.api.node.builder.ProcessBuilder;
import no.ssb.dc.api.services.Services;
import no.ssb.dc.api.util.JacksonFactory;
import no.ssb.dc.core.executor.BufferedReordering;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.executor.FixedThreadPool;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.handler.Queries;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerListener;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static no.ssb.dc.api.Builders.addContent;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.nextPage;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.parallel;
import static no.ssb.dc.api.Builders.process;
import static no.ssb.dc.api.Builders.publish;
import static no.ssb.dc.api.Builders.regex;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Listeners(TestServerListener.class)
public class GetTest {

    final String xml =
            "<?xml version=\"1.0\"?>" +
                    "<feed>" +
                    "    <entry>" +
                    "        <id>1</id>" +
                    "    </entry>" +
                    "    <entry>" +
                    "        <id>2</id>" +
                    "    </entry>" +
                    "    <entry>" +
                    "        <id>3</id>" +
                    "    </entry>" +
                    "</feed>";

    @Inject
    TestServer testServer;

    Get getListNode;

    ProcessBuilder processBuilder;
    private Services services;

    @BeforeMethod
    public void setUp() {
        processBuilder = process(ReturnNextPagePosition.class).output("nextPosition");
        getListNode = get("list")
                .url(testServer.testURL("/ns/mock?cursor=${fromPosition}&size=10"))
                .step(processBuilder)
                .build();

        services = Services.create()
                .register(ConfigurationMap.class, new ConfigurationMap(testServer.getConfiguration().asMap()))
                .register(Client.class, Client.newClientBuilder().build())
                .register(BufferedReordering.class, new BufferedReordering<>())
                .register(ContentStore.class, ProviderConfigurator.configure(testServer.getConfiguration().asMap(), "discarding", ContentStoreInitializer.class))
                .register(FixedThreadPool.class, FixedThreadPool.newInstance(testServer.getConfiguration()));
    }

    @Test
    public void thatGetConsumesAndProcessTheEndpoint() {
        ExecutionContext input = new ExecutionContext.Builder().services(services).build();

        input.variables().put("fromPosition", 1);

        ExecutionContext output = Executor.execute(getListNode, input);
        assertNotNull(output);

        assertEquals(output.variables().get("nextPosition"), 11L);
    }

    @Test
    public void thatGetSequenceAndParallelRespectsExpectedPositionsAndParallelRun() {
        Flow getPage = Flow.start("getPage", "page")
                .node(get("page")
                        .url(testServer.testURL("/ns/mock?seq=${fromPosition}&size=10"))
                        .step(sequence(xpath("/feed/entry"))
                                .expected(xpath("/entry/id"))
                        )
                        .step(parallel(xpath("/feed/entry"))
                                .variable("position", xpath("/entry/id"))
                                .step(execute("event-doc")
                                        .inputVariable("eventId", xpath("/entry/event/event-id"))
                                )
                                .step(publish("${position}"))
                        ))
                .node(get("event-doc")
                        .url(testServer.testURL("/ns/mock/${eventId}?type=event"))
                )
                .end();

        ExecutionContext input = new ExecutionContext.Builder().services(services).build();

        Headers requestHeaders = new Headers();
        requestHeaders.put("Accept", "application/xml");
        input.globalState(Headers.class, requestHeaders);

        input.variable("fromPosition", 1);

        ExecutionContext output = Executor.execute(getPage.startNode(), input);
        assertNotNull(output);
    }

    @Test
    public void thatPaginateHandlePages() throws InterruptedException {
        FlowBuilder builder =
                Flow.start("getPage", "page-loop")
//                .configuration().
                        .node(paginate("page-loop")
                                .variable("fromPosition", "${nextPosition}")
                                .addPageContent()
                                .step(execute("page"))
                                .prefetchThreshold(0.5)
                                .until(whenVariableIsNull("nextPosition"))
                        )
                        .node(get("page")
                                        .header("accept", "application/xml")
                                        .url(testServer.testURL("/ns/mock?seq=${fromPosition}&size=10"))
                                        .validate(
                                                status()
                                                        .success(200, 299)
                                                        .fail(400)
                                        )
                                        .positionProducer(LongPositionProducer.class)
                                        .step(sequence(xpath("/feed/entry"))
                                                .expected(xpath("/entry/id"))
                                        )
                                        .step(nextPage().output("nextPosition", regex(xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]seq=)[^&]*"))
//                                .output("nextPosition", eval(xpath("/feed/entry[last()]/id"), "result", "${cast.toLong(result) + 1}"))
                                        )
                                        .step(parallel(xpath("/feed/entry"))
                                                .variable("position", xpath("/entry/id"))
                                                .step(addContent("${position}", "entry"))
                                                .step(execute("event-doc")
                                                        .inputVariable("eventId", xpath("/entry/event/event-id"))
                                                )
                                                .step(publish("${position}"))
                                        )
                                        .returnVariables("nextPosition")
                        )
                        .node(get("event-doc")
                                .url(testServer.testURL("/ns/mock/${eventId}?type=event"))
                                .step(addContent("${position}", "event-doc"))
                        );

        ExecutionContext input = new ExecutionContext.Builder().services(services).build();
        input.variable("fromPosition", 1);

        Flow flow = builder.end();
        //System.out.printf("Config:%n%s%n", builder.serialize());
        //System.out.printf("ExecutionPlan:%n%s%n", flow.startNode().toPrintableExecutionPlan());

        Worker worker = new Worker(flow.startNode(), input);
        worker.run();
    }


    @Test
    public void thatProcessorReturnsNextPagePosition() {
        ExecutionContext input = ExecutionContext.empty().state(Response.class,
                new MockResponse("http://example.com", new Headers(), 200, "[{\"id\": \"1\"}, {\"id\": \"2\"}]".getBytes()));

        ExecutionContext output = Executor.execute(processBuilder.build(), input);
        assertNotNull(output);

        assertEquals(output.variables().get("nextPosition"), 3L);
    }

    @Test
    public void thatXpathReturnsList() {
        List<?> list = Queries.evaluate(xpath("/feed/entry").build()).queryList(xml.getBytes());

        assertEquals(list.size(), 3);
    }

    @Test
    public void thatXpathReturnsItem() {
        List<?> list = Queries.evaluate(xpath("/feed/entry").build()).queryList(xml.getBytes());
        List<String> positionsList = list.stream().map(item -> Queries.evaluate(xpath("/entry/id").build()).queryStringLiteral(item)).collect(Collectors.toList());

        assertEquals(positionsList.size(), 3);
    }

    static class MockResponse implements Response {

        private final String url;
        private final Headers headers;
        private final int statusCode;
        private final byte[] payload;
        private final Response previousResponse;

        public MockResponse(String url, Headers headers, int statusCode, byte[] payload) {
            this.url = url;
            this.headers = headers;
            this.statusCode = statusCode;
            this.payload = payload;
            this.previousResponse = null;
        }

        @Override
        public String url() {
            return url;
        }

        @Override
        public Headers headers() {
            return headers;
        }

        @Override
        public int statusCode() {
            return statusCode;
        }

        @Override
        public byte[] body() {
            return payload;
        }

        @Override
        public Optional<Response> previousResponse() {
            return Optional.ofNullable(previousResponse);
        }
    }

    static class ReturnNextPagePosition implements Processor {

        @Override
        public ExecutionContext process(ExecutionContext input) {
            Response response = input.state(Response.class);
            String body = new String(response.body());
            ArrayNode nodeList = JacksonFactory.instance().fromJson(body, ArrayNode.class);

            JsonNode lastNode = nodeList.get(nodeList.size() - 1);
            long lastPosition = lastNode.get("id").asLong();

            ExecutionContext output = ExecutionContext.empty();
            output.variables().put("nextPosition", lastPosition + 1L);

            return output;
        }
    }

    public static class LongPositionProducer implements PositionProducer<Long> {
        @Override
        public Position<Long> produce(String id) {
            return new Position<>(Long.valueOf(id));
        }
    }
}
