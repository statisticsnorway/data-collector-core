package no.ssb.dc.core.controller;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.undertow.io.Receiver;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import no.ssb.dc.api.util.JacksonFactory;
import no.ssb.dc.application.Controller;

import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;

public class LoopbackController implements Controller {

    @Override
    public String contextPath() {
        return "/echo";
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        if (exchange.isInIoThread()) {
                exchange.dispatch(this);
            return;
        }

        ObjectNode objectNode = JacksonFactory.instance().createObjectNode();

        {
            ObjectNode childObjectNode = JacksonFactory.instance().createObjectNode();
            exchange.getRequestHeaders().getHeaderNames().forEach(h -> {
                exchange.getRequestHeaders().eachValue(h).forEach(v -> {
                    childObjectNode.put(h.toString(), v);
                });
            });
            objectNode.set("request-headers", childObjectNode);
        }

        {
            ObjectNode childObjectNode = JacksonFactory.instance().createObjectNode();
            childObjectNode.put("uri", exchange.getRequestURI());
            childObjectNode.put("method", exchange.getRequestMethod().toString());
            childObjectNode.put("statusCode", String.valueOf(exchange.getStatusCode()));
            childObjectNode.put("isSecure", Boolean.valueOf(exchange.isSecure()).toString());
            childObjectNode.put("sourceAddress", exchange.getSourceAddress().toString());
//            childObjectNode.put("destinationAddress", exchange.getDestinationAddress().toString());
            objectNode.set("request-info", childObjectNode);
        }

        {
            ObjectNode childObjectNode = JacksonFactory.instance().createObjectNode();
            exchange.getRequestCookies().forEach((k, v) -> {
                childObjectNode.put(k, v.getValue());
            });
            objectNode.set("cookies", childObjectNode);
        }

        {
            ObjectNode childObjectNode = JacksonFactory.instance().createObjectNode();
            exchange.getPathParameters().entrySet().forEach((e) -> {
                childObjectNode.put(e.getKey(), e.getValue().element());
            });
            objectNode.set("path-parameters", childObjectNode);
        }

        {
            objectNode.put("queryString", exchange.getQueryString());
            ObjectNode childObjectNode = JacksonFactory.instance().createObjectNode();
            exchange.getQueryParameters().entrySet().forEach((e) -> {
                childObjectNode.put(e.getKey(), e.getValue().element());
            });
            objectNode.set("query-parameters", childObjectNode);
        }

        {
            objectNode.put("contentLength", String.valueOf(exchange.getRequestContentLength()));
            ObjectNode childObjectNode = JacksonFactory.instance().createObjectNode();
            exchange.getRequestReceiver().receiveFullBytes(new Receiver.FullBytesCallback() {
                @Override
                public void handle(HttpServerExchange httpServerExchange, byte[] bytes) {
                    childObjectNode.put("payload", new String(bytes));
                }
            });
            objectNode.set("request-body", childObjectNode);
        }

        {
            ObjectNode childObjectNode = JacksonFactory.instance().createObjectNode();
            exchange.getResponseHeaders().getHeaderNames().forEach(h -> {
                exchange.getResponseHeaders().eachValue(h).forEach(v -> {
                    childObjectNode.put(h.toString(), v);
                });
            });
            objectNode.set("response-headers", childObjectNode);
        }

        {
            ObjectNode childObjectNode = JacksonFactory.instance().createObjectNode();
            exchange.getResponseCookies().forEach((k, v) -> {
                childObjectNode.put(k, v.getValue());
            });
            objectNode.set("response-cookies", childObjectNode);
        }

        if ("GET".equalsIgnoreCase(exchange.getRequestMethod().toString()))
            exchange.setStatusCode(HTTP_OK);
        else if ("POST".equalsIgnoreCase(exchange.getRequestMethod().toString()))
            exchange.setStatusCode(HTTP_NO_CONTENT);
        else
            throw new UnsupportedOperationException("Method " + exchange.getRequestMethod() + " not supported!");

        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(JacksonFactory.instance().toPrettyJSON(objectNode));
    }
}
