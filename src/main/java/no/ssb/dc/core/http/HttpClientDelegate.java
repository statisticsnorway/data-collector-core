package no.ssb.dc.core.http;

import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Authenticator;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class HttpClientDelegate implements Client {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientDelegate.class);

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
                }).exceptionally(throwable -> {
                    LOG.error("HttpClient.sendAsync error:\n{}", CommonUtils.captureStackTrace(throwable));
                    throw new RuntimeException(throwable);
                });
    }

    @Override
    public Object getDelegate() {
        return httpClient;
    }

    public static class ClientBuilder implements Builder {

        private Version version = Version.HTTP_2;
        private int priority = 0;
        private Authenticator authenticator;
        private SSLContext sslContext;
        private SSLParameters sslParameters;
        private X509TrustManager trustManager; // unused for Java 11 HttpClient
        private Executor executor;
        private Duration duration;
        private Redirect redirectPolicy;
        private ProxySelector proxySelector;

        @Override
        public Builder version(Version version) {
            this.version = version;
            return this;
        }

        @Override
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        @Override
        public Builder authenticator(Authenticator authenticator) {
            this.authenticator = authenticator;
            return this;
        }

        @Override
        public Builder sslContext(SSLContext sslContext) {
            this.sslContext = sslContext;
            return this;
        }

        @Override
        public Builder sslParameters(SSLParameters sslParameters) {
            this.sslParameters = sslParameters;
            return this;
        }

        @Override
        public Builder x509TrustManager(X509TrustManager trustManager) {
            this.trustManager = trustManager;
            return this;
        }

        @Override
        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        @Override
        public Builder connectTimeout(Duration duration) {
            this.duration = duration;
            return this;
        }

        @Override
        public Builder followRedirects(Redirect policy) {
            redirectPolicy = policy;
            return this;
        }

        @Override
        public Builder proxy(ProxySelector proxySelector) {
            this.proxySelector = proxySelector;
            return this;
        }

        @Override
        public Client build() {
            HttpClient.Builder httpClientBuilder = HttpClient.newBuilder();

            switch (version) {
                case HTTP_1_1:
                    httpClientBuilder.version(HttpClient.Version.HTTP_1_1);
                    break;

                case HTTP_2:
                    httpClientBuilder.version(HttpClient.Version.HTTP_2);
                    break;
                default:
                    throw new IllegalStateException();
            }

            if (priority > 0) httpClientBuilder.priority(priority);

            if (authenticator != null) httpClientBuilder.authenticator(authenticator);

            if (sslContext != null) {
                httpClientBuilder.sslContext(sslContext);
            }

            if (sslParameters != null) httpClientBuilder.sslParameters(sslParameters);

            if (executor != null) httpClientBuilder.executor(executor);

            if (duration != null) httpClientBuilder.connectTimeout(duration);

            if (redirectPolicy != null) {
                switch (redirectPolicy) {
                    case NEVER:
                        httpClientBuilder.followRedirects(HttpClient.Redirect.NEVER);
                        break;

                    case ALWAYS:
                        httpClientBuilder.followRedirects(HttpClient.Redirect.ALWAYS);
                        break;

                    case NORMAL:
                        httpClientBuilder.followRedirects(HttpClient.Redirect.NORMAL);
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }

            if (proxySelector != null) httpClientBuilder.proxy(proxySelector);

            return new HttpClientDelegate(httpClientBuilder.build());
        }
    }

}
