package no.ssb.dc.core;

import no.ssb.dc.api.Specification;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.core.executor.Worker;
import no.ssb.dc.test.server.TestServer;
import no.ssb.dc.test.server.TestServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.util.Map;

import static no.ssb.dc.api.Builders.context;
import static no.ssb.dc.api.Builders.post;

@ExtendWith(TestServerExtension.class)
public class PostTest {

    @Inject
    TestServer testServer;

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
                        )
                )
                .configuration(Map.of("username", "user", "password", "pass"))
                .build()
                .run();
    }
}

