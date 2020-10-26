package no.ssb.dc.core.http;

import no.ssb.dc.api.http.BodyHandler;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import okhttp3.OkHttpClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Authenticator;
import java.net.ProxySelector;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class OkHttpClientDelegate implements Client {

    private final OkHttpClient client;

    public OkHttpClientDelegate(OkHttpClient client) {
        this.client = client;
    }

    @Override
    public Version version() {
        return Version.HTTP_1_1;
    }

    @Override
    public Response send(Request request) {
        okhttp3.Request httpRequest = (okhttp3.Request) request.getDelegate();
        try (okhttp3.Response httpResponse = client.newCall(httpRequest).execute()) {
            Response.Builder responseBuilder = Response.newResponseBuilder();
            responseBuilder.delegate(httpResponse);
            return responseBuilder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <R> Response send(Request request, BodyHandler<R> bodyHandler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Response> sendAsync(Request request) {
        return CompletableFuture.supplyAsync(() -> send(request));
    }

    @Override
    public <R> CompletableFuture<Response> sendAsync(Request request, BodyHandler<R> bodyHandler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getDelegate() {
        return client;
    }

    public static class ClientBuilder implements Builder {

        private Version version = Version.HTTP_2;
        private int priority = 0;
        private Authenticator authenticator;
        private SSLContext sslContext;
        private SSLParameters sslParameters;
        private X509TrustManager trustManager;
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
            OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder();

            switch (version) {
                case HTTP_1_1:
                    // override not supported
                    break;

                case HTTP_2:
                    // override not supported
                    break;
                default:
                    throw new IllegalStateException();
            }

            if (priority > 0) {
                // since override is not supported, HTTP/2 priority makes no sense
            }

            if (authenticator != null) {
                // todo httpClientBuilder.authenticator(authenticator);
                // incompatible class between kotlin and java
            }

            if (sslContext != null) {
                httpClientBuilder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
            }

            if (sslParameters != null) {
                // not supported
            }

            if (executor != null) {
                throw new UnsupportedOperationException();
            }

            if (duration != null) httpClientBuilder.connectTimeout(duration);

            if (redirectPolicy != null) {
                switch (redirectPolicy) {
                    case NEVER:
                        httpClientBuilder.followRedirects(false);
                        httpClientBuilder.followSslRedirects(false);
                        break;

                    case ALWAYS:
                    case NORMAL:
                        httpClientBuilder.followRedirects(true);
                        httpClientBuilder.followSslRedirects(true);
                        break;

                    default:
                        throw new IllegalStateException();
                }
            }

            if (proxySelector != null) httpClientBuilder.proxySelector(proxySelector);

            return new OkHttpClientDelegate(httpClientBuilder.build());
        }
    }
}
