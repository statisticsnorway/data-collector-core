package no.ssb.dc.core.http;

import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Request;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

        @Override
        public Request.Builder url(String url) {
            this.url = url;
            return this;
        }

        @Override
        public Request.Builder PUT() {
            this.method = Method.PUT;
            return this;
        }

        @Override
        public Request.Builder POST() {
            this.method = Method.POST;
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
                    httpRequestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(null));
                    break;

                case POST:
                    httpRequestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(null));
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

            return new HttpRequestDelegate(httpRequestBuilder.build());
        }

    }
}
