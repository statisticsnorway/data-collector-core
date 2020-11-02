package no.ssb.dc.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import net.bytebuddy.agent.ByteBuddyAgent;
import no.ssb.dc.api.Processor;
import no.ssb.dc.api.Specification;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.ContentStoreInitializer;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.builder.SpecificationBuilder;
import no.ssb.dc.api.util.JsonParser;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.core.handler.Queries;
import no.ssb.dc.core.metrics.MetricsAgent;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerExtension;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static no.ssb.dc.api.Builders.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(TestServerExtension.class)
public class GetTest {

    static final Logger LOG = LoggerFactory.getLogger(GetTest.class);

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

    @BeforeAll
    static void beforeAll() {
        MetricsAgent.premain(null, ByteBuddyAgent.install());
    }

    @AfterAll
    static void afterAll() throws IOException {
        StringWriter sw = new StringWriter();
        TextFormat.write004(sw, CollectorRegistry.defaultRegistry.metricFamilySamples());
        System.out.printf("%s%n", sw);
    }

    @Test
    public void thatGetConsumesAndProcessTheEndpoint() {
        ExecutionContext output = Worker.newBuilder()
                .specification(get("list")
                        .url(testServer.testURL("/api/events?position=${fromPosition}&pageSize=10"))
                        .pipe(process(ReturnNextPagePosition.class)
                                .output("nextPosition")
                        )
                )
                .variable("fromPosition", 1)
                .build()
                .run();

        assertNotNull(output);
        assertEquals(output.variables().get("nextPosition"), 11L);
    }

    @Test
    public void thatGetSequenceAndParallelRespectsExpectedPositionsAndParallelRun() {
        ExecutionContext output = Worker.newBuilder()
                .specification(Specification.start("test", "getPage", "page")
                        .function(get("page")
                                .url(testServer.testURL("/api/events?position=${fromPosition}&pageSize=10"))
                                .pipe(sequence(xpath("/feed/entry"))
                                        .expected(xpath("/entry/id"))
                                )
                                .pipe(parallel(xpath("/feed/entry"))
                                        .variable("position", xpath("/entry/id"))
                                        .pipe(execute("event-doc")
                                                .inputVariable("eventId", xpath("/entry/event-id"))
                                        )
                                        .pipe(publish("${position}"))
                                ))
                        .function(get("event-doc")
                                .url(testServer.testURL("/api/events/${eventId}?type=event"))
                        )
                )
                .configuration(Map.of("content.stream.connector", "discarding"))
                .header("Accept", "application/xml")
                .variable("fromPosition", 1)
                .build()
                .run();

        assertNotNull(output);
    }

    @Test
    public void thatGetEmptyFeed() {
        ExecutionContext output = Worker.newBuilder()
                .specification(Specification.start("test", "getPage", "loop")
                        .function(paginate("loop")
                                .variable("fromPosition", "${nextPosition}")
                                .addPageContent("fromPosition")
                                .iterate(execute("feed"))
                                .prefetchThreshold(150)
                                .until(whenVariableIsNull("nextPosition"))
                        )
                        .function(get("feed")
                                .url(testServer.testURL("/api/events?position=${fromPosition}&pageSize=0"))
                                .pipe(sequence(xpath("/feed/entry"))
                                        .expected(xpath("/entry/id"))
                                )
                        )
                )
                .configuration(Map.of("content.stream.connector", "discarding"))
                //.configuration(Map.of("content.stream.connector", "rawdata", "rawdata.client.provider", "memory"))
                .header("Accept", "application/xml")
                .variable("fromPosition", 1)
                .build()
                .run();

        assertNotNull(output);
    }

    @Test
    public void thatGetEmptyFeedWithRequestTimeoutSetToZero() {
        assertThrows(CompletionException.class, () -> {
            ExecutionContext output = Worker.newBuilder()
                    .specification(Specification.start("test", "getPage", "loop")
                            .function(paginate("loop")
                                    .variable("fromPosition", "${nextPosition}")
                                    .addPageContent("fromPosition")
                                    .iterate(execute("feed"))
                                    .prefetchThreshold(150)
                                    .until(whenVariableIsNull("nextPosition"))
                            )
                            .function(get("feed")
                                    .url(testServer.testURL("/api/events?position=${fromPosition}&pageSize=0"))
                                    .pipe(sequence(xpath("/feed/entry"))
                                            .expected(xpath("/entry/id"))
                                    )
                            )
                    )
                    .configuration(Map.of("content.stream.connector", "discarding", "data.collector.http.request.timeout.seconds", "0"))
                    .header("Accept", "application/xml")
                    .variable("fromPosition", 1)
                    .build()
                    .run();

            assertNotNull(output);
        });
    }

    @Test
    public void thatGetConsumesAndValidateCustom404Error() {
        final SpecificationBuilder specificationBuilder = Specification.start("test", "getPage", "page")
                .function(get("page")
                        .url(testServer.testURL("/api/events?position=${fromPosition}&pageSize=10"))
                        .pipe(sequence(xpath("/feed/entry"))
                                .expected(xpath("/entry/id"))
                        )
                        .pipe(parallel(xpath("/feed/entry"))
                                .variable("position", xpath("/entry/id"))
                                .pipe(execute("event-doc")
                                        .inputVariable("eventId", xpath("/entry/event-id"))
                                )
                                .pipe(publish("${position}"))
                                .pipe(addContent("${position}", "entry"))
                        ))
                .function(get("event-doc")
                        .url(testServer.testURL("/api/events/${eventId}?type=event&404withResponseError"))
                        // TODO fix incomplete impl
                        .validate(status()
                                .success(200)
                                .success(404, bodyContains(xpath("/feil/kode"), "SM-002")))
                        .pipe(addContent("${position}", "event-doc-error"))
                );


//        LOG.trace("{}", JsonParser.createJsonParser().toPrettyJSON(specificationBuilder));

//        if (true) return;

        ExecutionContext output = Worker.newBuilder()
                .specification(specificationBuilder
                )
                .configuration(Map.of("content.stream.connector", "discarding"))
                .header("Accept", "application/xml")
                .variable("fromPosition", 1)
                .build()
                .run();

        assertNotNull(output);
    }


    // replicate freg bulk uttrekk wait for false positive status code
    @Test
    public void thatGetConsumesAndWaitUntilFalsePositiveOnCustom404Error() {
        final SpecificationBuilder specification = Specification.start("test", "getPage", "page")
                .function(get("page")
                        .url(testServer.testURL("/api/events?position=${fromPosition}&pageSize=10"))
                        .retryWhile(statusCode().is(404, TimeUnit.SECONDS, 15)
                                //.bodyContains(regex(jqpath(".feilmelding"), "^(Batch med id=\\d+ er enda ikke klar)$"))
                        )
                        .validate(status().success(200))
                        .pipe(sequence(xpath("/feed/entry"))
                                .expected(xpath("/entry/id"))
                        )
                        .pipe(parallel(xpath("/feed/entry"))
                                .variable("position", xpath("/entry/id"))
                                .pipe(addContent("${position}", "entry"))
                                .pipe(publish("${position}"))
                        ));


        //LOG.trace("{}", JsonParser.createJsonParser().toPrettyJSON(specification));

        String json = JsonParser.createJsonParser().toPrettyJSON(specification);

        SpecificationBuilder spec = Specification.deserialize(json);


        if (true) return;

        ExecutionContext output = Worker.newBuilder()
                .specification(specification
                )
                .configuration(Map.of("content.stream.connector", "discarding"))
                .header("Accept", "application/xml")
                .variable("fromPosition", 1)
                .build()
                .run();

        assertNotNull(output);
    }

    @Test
    public void thatPaginateHandlePages() throws Exception {
        SpecificationBuilder specificationBuilder = Specification.start("test", "getPage", "page-loop")
                .configure(context()
                        .topic("topic")
                        .header("accept", "application/xml")
                        .variable("baseURL", testServer.testURL(""))
                        .variable("nextPosition", "${contentStream.lastOrInitialPosition(1)}")
                )
                .function(paginate("page-loop")
                        .variable("fromPosition", "${nextPosition}")
                        .addPageContent("fromPosition")
                        .iterate(execute("page"))
                        .prefetchThreshold(5)
                        .until(whenVariableIsNull("nextPosition"))
                )
                .function(get("page")
                        .url("${baseURL}/api/events?position=${fromPosition}&pageSize=10")
                        .validate(status().success(200, 299).fail(300, 599))
                        .pipe(sequence(xpath("/feed/entry"))
                                .expected(xpath("/entry/id"))
                        )
                        .pipe(nextPage()
                                        .output("nextPosition", regex(xpath("/feed/link[@rel=\"next\"]/@href"), "(?<=[?&]position=)[^&]*"))
                                //.output("nextPosition", eval(xpath("/feed/entry[last()]/id"), "result", "${cast.toLong(result) + 1}"))
                        )
                        .pipe(parallel(xpath("/feed/entry"))
                                .variable("position", xpath("/entry/id"))
                                .pipe(addContent("${position}", "entry"))
                                .pipe(execute("event-doc")
                                        .inputVariable("eventId", xpath("/entry/event-id"))
                                )
                                .pipe(publish("${position}"))
                        )
                        .returnVariables("nextPosition")
                )
                .function(get("event-doc")
                        .url("${baseURL}/api/events/${eventId}?type=event")
                        .pipe(addContent("${position}", "event-doc"))
                );

        Map<String, String> configurationMap = Map.of("content.stream.connector", "rawdata", "rawdata.client.provider", "memory");
        ContentStore contentStore = ProviderConfigurator.configure(configurationMap, configurationMap.get("content.stream.connector"), ContentStoreInitializer.class);

        Worker.WorkerBuilder worker = Worker.newBuilder()
                .specification(specificationBuilder)
                .configuration(Map.of("content.stream.connector", "rawdata", "rawdata.client.provider", "memory"))
                .contentStore(contentStore)
                .keepContentStoreOpenOnWorkerCompletion(true);

        ExecutionContext output = worker.stopAtNumberOfIterations(5).build().run();
        assertNotNull(output);

        Worker worker2 = worker.build();
        worker2.resetMaxNumberOfIterations();
        ExecutionContext output2 = worker2.run();
        assertNotNull(output2);

        contentStore.close();
    }

    @Test
    public void thatProcessorReturnsNextPagePosition() {
        ExecutionContext input = ExecutionContext.empty().state(Response.class,
                new MockResponse("http://example.com", new Headers(), 200, "[{\"id\": \"1\"}, {\"id\": \"2\"}]".getBytes()));

        ExecutionContext output = Executor.execute(process(ReturnNextPagePosition.class).output("nextPosition").build(), input);
        assertNotNull(output);

        assertEquals(output.variables().get("nextPosition"), 3L);
    }

    @Test
    public void thatXpathReturnsList() {
        List<?> list = Queries.from(xpath("/feed/entry").build()).evaluateList(xml.getBytes());

        assertEquals(list.size(), 3);
    }

    @Test
    public void thatXpathReturnsItem() {
        List<?> list = Queries.from(xpath("/feed/entry").build()).evaluateList(xml.getBytes());
        List<String> positionsList = list.stream().map(item -> Queries.from(xpath("/entry/id").build()).evaluateStringLiteral(item)).collect(Collectors.toList());

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
        public Optional<Flow.Subscriber<List<ByteBuffer>>> bodyHandler() {
            return Optional.empty();
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
            ArrayNode nodeList = JsonParser.createJsonParser().fromJson(body, ArrayNode.class);

            JsonNode lastNode = nodeList.get(nodeList.size() - 1);
            long lastPosition = lastNode.get("id").asLong();

            ExecutionContext output = ExecutionContext.empty();
            output.variables().put("nextPosition", lastPosition + 1L);

            return output;
        }
    }

}
