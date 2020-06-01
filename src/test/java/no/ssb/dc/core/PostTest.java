package no.ssb.dc.core;

import no.ssb.dc.api.Specification;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.BodyPublisher;
import no.ssb.dc.api.node.FormEncoding;
import no.ssb.dc.api.node.builder.BodyPublisherBuilder;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.Map;

import static no.ssb.dc.api.Builders.bodyPublisher;
import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.execute;
import static no.ssb.dc.api.Builders.get;
import static no.ssb.dc.api.Builders.jqpath;
import static no.ssb.dc.api.Builders.paginate;
import static no.ssb.dc.api.Builders.post;
import static no.ssb.dc.api.Builders.sequence;
import static no.ssb.dc.api.Builders.status;
import static no.ssb.dc.api.Builders.whenVariableIsNull;
import static no.ssb.dc.api.Builders.xpath;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(TestServerExtension.class)
public class PostTest {

    @Inject
    TestServer testServer;

    @Test
    void buildBodyPublisher() {
        BodyPublisherBuilder builder = bodyPublisher();
        builder.urlEncodedData("foo=bar&foobar=baz");
        BodyPublisher bodyPublisher = builder.build(null);
        assertEquals(FormEncoding.APPLICATION_X_WWW_FORM_URLENCODED, bodyPublisher.getEncoding());
        assertEquals("foo=bar&foobar=baz", bodyPublisher.getUrlEncodedData());
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
                                        .urlEncodedData("user=${ENV.username}&password=${ENV.password}")
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
                .configuration(Map.of("username", "user", "password", "pass"))
                .build()
                .run();
    }
}

