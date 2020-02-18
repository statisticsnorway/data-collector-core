package no.ssb.dc.core.controller;

import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.config.DynamicConfiguration;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.application.server.Controller;
import no.ssb.dc.core.service.TestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class TestController implements Controller {

    private static final Logger LOG = LoggerFactory.getLogger(TestController.class);
    private final DynamicConfiguration configuration;
    private final TestService testService;

    public TestController(DynamicConfiguration configuration, TestService testService) {
        this.configuration = configuration;
        this.testService = testService;
    }

    @Override
    public String contextPath() {
        return "/test";
    }

    @Override
    public Set<Request.Method> allowedMethods() {
        return Set.of(Request.Method.GET);
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
            exchange.dispatch(this);
            return;
        }

        if (true) {
            exchange.setStatusCode(200);
            return;
        }

        exchange.setStatusCode(404);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        exchange.getResponseSender().send("Not found: " + exchange.getRequestPath());
    }

}
