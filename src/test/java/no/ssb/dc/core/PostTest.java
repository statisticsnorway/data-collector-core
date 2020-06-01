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
import static no.ssb.dc.api.Builders.post;
import static no.ssb.dc.api.Builders.status;
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
                                .variable("uid", "${username}")
                                .variable("pwd", "${password}")
                        )
                        .function(post("authorize")
                                .url(testServer.testURL("/api/authorize"))
                                .data(bodyPublisher()
                                        .urlEncodedData("user=user&password=pass")
                                )
                                .validate(status().success(200, 299))
                                // todo parse response and bind to variable
//                                .pipe(paginate("loop")
//                                        .variable("accessToken", "${accessToken}")
//                                        .iterate(execute("page"))
//                                        .prefetchThreshold(150)
//                                        .until(whenVariableIsNull("nextSequence"))
//                                )
                        )
                )
                .configuration(Map.of("username", "user", "password", "pass"))
                .build()
                .run();
    }
}

