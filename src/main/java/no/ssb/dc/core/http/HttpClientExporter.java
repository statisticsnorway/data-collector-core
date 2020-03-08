package no.ssb.dc.core.http;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

class HttpClientExporter {

    static final Gauge requestStartedCountGauge = Gauge.build("request_started", "Started requests")
            .namespace("dc")
            .subsystem("client")
            //.labelNames("location", "path")
            .register();

    static final Gauge requestCompletedCountGauge = Gauge.build("request_completed", "Completed requests")
            .namespace("dc")
            .subsystem("client")
            //.labelNames("location", "path", "statusCode", "emptyResponse")
            .register();

    static final Histogram requestDurationHistogram = Histogram.build("request_duration", "Request duration")
            .namespace("dc")
            .subsystem("client")
            //.labelNames("location", "path")
            .register();

    static class Send {
        static Response intercept(@SuperCall Callable<Response> zuper, @Argument(0) Request request) throws Exception {
            //URLInfo urlInfo = new URLInfo(request.url());
            Histogram.Timer timer = requestDurationHistogram/*.labels(urlInfo.getLocation(), urlInfo.getRequestPath())*/.startTimer();
            try {
                requestStartedCountGauge/*.labels(urlInfo.getLocation(), urlInfo.getRequestPath())*/.inc();
                Response response = zuper.call();
                //String statusCode = response == null ? "-1" : String.valueOf(response.statusCode());
                //String emptyResponse = response == null || !(response.body() != null && response.body().length > 0) ? "true" : "false";
                requestCompletedCountGauge/*.labels(urlInfo.getLocation(), urlInfo.getRequestPath(), statusCode, emptyResponse)*/.inc();
                return response;
            } finally {
                timer.observeDuration();
            }
        }
    }

    static class SendAsync {
        static CompletableFuture<Response> intercept(@SuperCall Callable<CompletableFuture<Response>> zuper, @Argument(0) Request request) throws Exception {
            //URLInfo urlInfo = new URLInfo(request.url());
            Histogram.Timer timer = requestDurationHistogram/*.labels(urlInfo.getLocation(), urlInfo.getRequestPath())*/.startTimer();
            requestStartedCountGauge/*.labels(urlInfo.getLocation(), urlInfo.getRequestPath())*/.inc();
            return zuper.call()
                    .thenApply(response -> {
                        //String statusCode = response == null ? "-1" : String.valueOf(response.statusCode());
                        //String emptyResponse = response == null || !(response.body() != null && response.body().length > 0) ? "true" : "false";
                        requestCompletedCountGauge/*.labels(urlInfo.getLocation(), urlInfo.getRequestPath(), statusCode, emptyResponse)*/.inc();
                        return response;
                    })
                    .thenApply(response -> {
                        timer.observeDuration();
                        return response;
                    });
        }
    }

    static class URLInfo {
        private final URL url;

        URLInfo(String url) {
            try {
                this.url = new URL(url);
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }

        String getLocation() {
            return String.format("%s://%s%s", url.getProtocol(), url.getHost(), (-1 == url.getPort() || 80 == url.getPort() || 443 == url.getPort() ? "" : ":" + url.getPort()));
        }

        String getRequestPath() {
            return url.getPath();
        }
    }
}
