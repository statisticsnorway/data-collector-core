package no.ssb.dc.core.http;

import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.HttpStatus;
import no.ssb.dc.api.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class OkHttpResponseDelegate implements Response {

    final String url;
    final Headers headers;
    final int statusCode;
    final byte[] payload;
    final Response previousResponse;

    public OkHttpResponseDelegate(String url, Headers headers, int statusCode, byte[] payload, Response previousResponse) {
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

    public static class ResponseBuilder implements Builder {

        private static final Logger LOG = LoggerFactory.getLogger(ResponseBuilder.class);

        okhttp3.Response httpResponse;

        @Override
        public Builder delegate(Object delegate) {
            this.httpResponse = (okhttp3.Response) delegate;
            return this;
        }

        @Override
        public Response build() {
            try {
                if (httpResponse == null) {
                    return new OkHttpResponseDelegate(
                            "",
                            new Headers(new LinkedHashMap<>()),
                            HttpStatus.HTTP_NOT_ACCEPTABLE.code(),
                            new byte[0],
                            null
                    );
                } else {
                    String body = httpResponse.body().string();

                    return new OkHttpResponseDelegate(
                            httpResponse.request().url().toString(),
                            new Headers(httpResponse.headers().toMultimap()),
                            httpResponse.code(),
                            Arrays.copyOf(body.getBytes(), body.getBytes().length),
                            null
                    );
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
