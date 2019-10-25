package no.ssb.dc.core.http;

import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

public class HttpClientDelegate implements Client {

    final HttpClient httpClient;

    private HttpClientDelegate(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Response send(Request request) {
        try {
            HttpResponse<byte[]> httpResponse = httpClient.send((HttpRequest) request.getDelegate(), HttpResponse.BodyHandlers.ofByteArray());
            Response.Builder responseBuilder = Response.newResponseBuilder();
            responseBuilder.delegate(httpResponse);
            return responseBuilder.build();

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Response> sendAsync(Request request) {
        return httpClient.sendAsync((HttpRequest) request.getDelegate(), HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(httpResponse -> {
                    Response.Builder responseBuilder = Response.newResponseBuilder();
                    responseBuilder.delegate(httpResponse);
                    return responseBuilder.build();
                });
    }

    @Override
    public Object getDelegate() {
        return httpClient;
    }

    public static class ClientBuilder implements Builder {

        Version version = Version.HTTP_2;
        SSLContext sslContext;

        @Override
        public Builder version(Version version) {
            this.version = version;
            return this;
        }

        @Override
        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        @Override
        public Client build() {
            HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();

            if (sslContext != null) {
                httpClientBuilder.sslContext(sslContext);
            }

            switch (version) {
                case HTTP_1_1:
                    httpClientBuilder.version(HttpClient.Version.HTTP_1_1);
                    break;

                case HTTP_2:
                    httpClientBuilder.version(HttpClient.Version.HTTP_2);
                    break;
            }

            return new HttpClientDelegate(httpClientBuilder.build());
        }
    }

}
