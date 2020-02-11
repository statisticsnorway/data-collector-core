package no.ssb.dc.core.http;

import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Response;

import java.util.Optional;

public class OkHttpResponseDelegate implements Response {

    @Override
    public String url() {
        return null;
    }

    @Override
    public Headers headers() {
        return null;
    }

    @Override
    public int statusCode() {
        return 0;
    }

    @Override
    public byte[] body() {
        return new byte[0];
    }

    @Override
    public Optional<Response> previousResponse() {
        return Optional.empty();
    }

    public static class ResponseBuilder implements Builder {

        @Override
        public Builder delegate(Object delegate) {
            return null;
        }

        @Override
        public Response build() {
            return null;
        }
    }
}
