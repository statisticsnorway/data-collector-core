package no.ssb.dc.core.http;

import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Request;

import java.time.Duration;

public class OkHttpRequestDelegate implements Request {
    @Override
    public String url() {
        return null;
    }

    @Override
    public Method method() {
        return null;
    }

    @Override
    public Headers headers() {
        return null;
    }

    @Override
    public Object getDelegate() {
        return null;
    }

    public static class RequestBuilder implements Request.Builder {

        @Override
        public Builder url(String url) {
            return null;
        }

        @Override
        public Builder PUT() {
            return null;
        }

        @Override
        public Builder POST() {
            return null;
        }

        @Override
        public Builder GET() {
            return null;
        }

        @Override
        public Builder DELETE() {
            return null;
        }

        @Override
        public Builder header(String name, String value) {
            return null;
        }

        @Override
        public Builder expectContinue(boolean enable) {
            return null;
        }

        @Override
        public Builder timeout(Duration duration) {
            return null;
        }

        @Override
        public Request build() {
            return null;
        }
    }
}
