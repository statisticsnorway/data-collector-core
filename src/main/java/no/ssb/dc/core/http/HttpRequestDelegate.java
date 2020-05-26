package no.ssb.dc.core.http;

import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Request;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

public class HttpRequestDelegate implements Request {

    final HttpRequest httpRequest;

    private HttpRequestDelegate(HttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    @Override
    public String url() {
        return httpRequest.uri().toString();
    }

    @Override
    public Method method() {
        return Method.valueOf(httpRequest.method());
    }

    @Override
    public Headers headers() {
        return new Headers(Collections.unmodifiableMap(httpRequest.headers().map()));
    }

    public Object getDelegate() {
        return httpRequest;
    }

    public static class RequestBuilder implements Request.Builder {

        String url;
        Request.Method method;
        Headers headers = new Headers();
        boolean enableExpectContinue;
        Duration timeoutDuration;
        byte[] payloadBytes;
        Flow.Publisher<ByteBuffer> bodyPublisher;

        @Override
        public Request.Builder url(String url) {
            this.url = url;
            return this;
        }

        @Override
        public Request.Builder PUT(byte[] bytes) {
            this.method = Method.PUT;
            payloadBytes = bytes;
            return this;
        }

        @Override
        public Builder PUT(Flow.Publisher<ByteBuffer> bodyPublisher) {
            this.method = Method.PUT;
            this.bodyPublisher = bodyPublisher;
            return this;
        }

        @Override
        public Request.Builder POST(byte[] bytes) {
            this.method = Method.POST;
            payloadBytes = bytes;
            return this;
        }

        @Override
        public Builder POST(Flow.Publisher<ByteBuffer> bodyPublisher) {
            this.method = Method.POST;
            this.bodyPublisher = bodyPublisher;
            return this;
        }

        @Override
        public Request.Builder GET() {
            this.method = Method.GET;
            return this;
        }

        @Override
        public Request.Builder DELETE() {
            this.method = Method.DELETE;
            return this;
        }

        @Override
        public Request.Builder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        @Override
        public Builder expectContinue(boolean enable) {
            enableExpectContinue = enable;
            return this;
        }

        @Override
        public Builder timeout(Duration duration) {
            this.timeoutDuration = duration;
            return this;
        }

        private void validate(Object... objects) {
            if (!Arrays.stream(objects).allMatch(Objects::nonNull)) {
                throw new RuntimeException("Null value");
            }
        }

        @Override
        public Request build() {
            validate(url, method);

            HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder(URI.create(url));

            switch (method) {
                case PUT:
                    if (payloadBytes != null) {
                        httpRequestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(payloadBytes));

                    } else if (bodyPublisher != null) {
                        httpRequestBuilder.PUT((HttpRequest.BodyPublisher) bodyPublisher);

                    } else {
                        throw new IllegalStateException("PUT: either payloadBytes OR boydPublisher must be set!");
                    }

                    break;

                case POST:
                    if (payloadBytes != null) {
                        httpRequestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(payloadBytes));

                    } else if (bodyPublisher != null) {
                        httpRequestBuilder.POST((HttpRequest.BodyPublisher) bodyPublisher);

                    } else {
                        throw new IllegalStateException("POST: either payloadBytes OR boydPublisher must be set!");
                    }
                    break;

                case GET:
                    httpRequestBuilder.GET();
                    break;

                case DELETE:
                    httpRequestBuilder.DELETE();
                    break;
            }

            // copy request-headers to HttpRequest
            for (Map.Entry<String, List<String>> entry : headers.asMap().entrySet()) {
                entry.getValue().forEach(value -> httpRequestBuilder.header(entry.getKey(), value));
            }

            httpRequestBuilder.expectContinue(enableExpectContinue);

            if (timeoutDuration != null) httpRequestBuilder.timeout(timeoutDuration);

            return new HttpRequestDelegate(httpRequestBuilder.build());
        }

    }
}
