package no.ssb.dc.core.http;

import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.HttpStatusCode;
import no.ssb.dc.api.http.Response;

import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class HttpResponseDelegate implements Response {

    final String url;
    final Headers headers;
    final int statusCode;
    final byte[] payload;
    final Response previousResponse;

    private HttpResponseDelegate(String url, Headers headers, int statusCode, byte[] payload, Response previousResponse) {
        this.url = url;
        this.headers = headers;
        this.statusCode = statusCode;
        this.payload = payload;
        this.previousResponse = previousResponse;
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
        return ofNullable(previousResponse);
    }

    @SuppressWarnings("unchecked")
    public static class ResponseBuilder implements Builder {

        HttpResponse<byte[]> httpResponse;

        @Override
        public Builder delegate(Object delegate) {
            this.httpResponse = (HttpResponse<byte[]>) delegate;
            return this;
        }

        private Response previousResponse() {
            if (httpResponse != null && httpResponse.previousResponse().isPresent()) {
                HttpResponse<byte[]> response = httpResponse.previousResponse().orElseThrow();
                return Response.newResponseBuilder().delegate(response).build();
            }
            return null;
        }

        @Override
        public Response build() {
            return new HttpResponseDelegate(
                    httpResponse != null ? httpResponse.uri().toString()  : "",
                    httpResponse != null ? new Headers(httpResponse.headers().map()) : new Headers(new LinkedHashMap<>()),
                    httpResponse != null ? httpResponse.statusCode() : HttpStatusCode.HTTP_NOT_ACCEPTABLE.statusCode(),
                    httpResponse != null ? httpResponse.body() : new byte[0],
                    previousResponse()
            );
        }
    }

}
