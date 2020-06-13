package no.ssb.dc.core;

import no.ssb.dc.api.Specification;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.BodyPublisher;
import no.ssb.dc.api.node.Configurations;
import no.ssb.dc.api.node.FormEncoding;
import no.ssb.dc.api.node.builder.BodyPublisherBuilder;
import no.ssb.dc.api.node.builder.BuildContext;
import no.ssb.dc.api.node.builder.JwtBuilder;
import no.ssb.dc.api.node.builder.JwtClaims;
import no.ssb.dc.api.node.builder.JwtHeaderClaims;
import no.ssb.dc.api.node.builder.SecurityBuilder;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;

import static no.ssb.dc.api.Builders.bodyPublisher;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.jqpath;
import static no.ssb.dc.api.Builders.jwtToken;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.post;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;
import static no.ssb.dc.api.node.builder.SpecificationBuilder.GLOBAL_CONFIGURATION;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(TestServerExtension.class)
public class PostTest {

    private static final Logger LOG = LoggerFactory.getLogger(PostTest.class);

    @Inject
    TestServer testServer;

    @Test
    void buildBodyPublisher() {
        BodyPublisherBuilder builder = bodyPublisher();
        builder.urlEncoded("foo=bar&foobar=baz");
        BodyPublisher bodyPublisher = builder.build(null);
        assertEquals(FormEncoding.APPLICATION_X_WWW_FORM_URLENCODED, bodyPublisher.getEncoding());
//        assertEquals("foo=bar&foobar=baz", bodyPublisher.getUrlEncodedData().produce(ExecutionContext.empty()));
    }

    @Disabled
    @Test
    void buildIdentityBodyPublisher() {
        BodyPublisherBuilder bodyPublisherBuilder = bodyPublisher();
        bodyPublisherBuilder.urlEncoded(jwtToken().identityId("test").bindTo("JWT_GRANT").token("grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${JWT_GRANT}"));

        Configurations.Builder configurationsBuilder = new Configurations.Builder();
        SecurityBuilder.SecurityNode securityNode = new SecurityBuilder().identity(new JwtBuilder("test", new JwtHeaderClaims(), new JwtClaims())).build();
        configurationsBuilder.add(securityNode);

        Map<String, Object> nodeInstanceById = new LinkedHashMap<>();
        nodeInstanceById.put(GLOBAL_CONFIGURATION, configurationsBuilder.build());
        BuildContext buildContext = BuildContext.of(new LinkedHashMap<>(), nodeInstanceById);

        BodyPublisher bodyPublisher = bodyPublisherBuilder.build(buildContext);

        assertEquals(FormEncoding.APPLICATION_X_WWW_FORM_URLENCODED, bodyPublisher.getEncoding());
//        assertEquals("foo=bar&foobar=baz", bodyPublisher.getUrlEncodedData().produce());

        ExecutionContext context = ExecutionContext.empty();

//        LOG.trace("produce: {}", bodyPublisher.getUrlEncodedData().produce(context));
        LOG.trace("serialized: {}", bodyPublisherBuilder.serialize());
    }

    @Test
    void authorizeResource() {
        ExecutionContext output = Worker.newBuilder()
                .specification(Specification.start("AUTHORIZE", "Authorize", "authorize")
                        .configure(context()
                                .topic("topic")
                                .variable("fromPosition", 1)
                        )
                        .function(post("authorize")
                                .url(testServer.testURL("/api/authorize"))
                                .data(bodyPublisher()
                                        .urlEncoded("grant_type=password&user=${ENV.ns_username}&password=${ENV.ns_password}")
                                )
                                .validate(status().success(200, 299))
                                .pipe(execute("loop")
                                        .inputVariable("accessToken", jqpath(".access_token"))
                                )
                        )
                        .function(paginate("loop")
                                .variable("fromPosition", "${nextPosition}")
                                .addPageContent("fromPosition")
                                .iterate(execute("page")
                                        .requiredInput("accessToken")
                                )
                                .prefetchThreshold(150)
                                .until(whenVariableIsNull("nextPosition"))
                        )
                        .function(get("page")
                                .header("Accept", "application/xml")
                                .header("Authorization", "Bearer ${accessToken}")
                                .url(testServer.testURL("/api/events?position=${fromPosition}&pageSize=0"))
                                .pipe(sequence(xpath("/feed/entry"))
                                        .expected(xpath("/entry/id"))
                                )
                        )
                )
                .configuration(Map.of("ns_username", "user", "ns_password", "pass"))
                .build()
                .run();
    }
}

