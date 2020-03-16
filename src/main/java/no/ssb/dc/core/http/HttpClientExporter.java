package no.ssb.dc.core.http;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Metrics:
 * <p>
 * request_started
 * request_completed
 * request_failure
 * request_duration
 */
class HttpClientExporter {

    static final Counter requestStartedCount = Counter.build("request_started", "Started requests")
            .namespace("dc")
            .subsystem("client")
            .labelNames("location")
            .register();

    static final Counter requestCompletedCount = Counter.build("request_completed", "Completed requests")
            .namespace("dc")
            .subsystem("client")
            .labelNames("location", "statusCode", "emptyResponse")
            .register();

    static final Counter requestFailureCount = Counter.build("request_failure", "Failed requests")
            .namespace("dc")
            .subsystem("client")
            .labelNames("location")
            .register();

    static final Histogram requestDurationHistogram = Histogram.build("request_duration", "Request duration")
            .namespace("dc")
            .subsystem("client")
            .labelNames("location")
            .register();

    static class Send {
        static Response intercept(@SuperCall Callable<Response> zuper, @Argument(0) Request request) throws Exception {
            URLInfo urlInfo = new URLInfo(request.url());
            Histogram.Timer timer = requestDurationHistogram.labels(urlInfo.getLocation()).startTimer();
            try {
                requestStartedCount.labels(urlInfo.getLocation()).inc();
                Response response = zuper.call();
                String statusCode = response == null ? "-1" : String.valueOf(response.statusCode());
                String emptyResponse = response == null || !(response.body() != null && response.body().length > 0) ? "true" : "false";
                requestCompletedCount.labels(urlInfo.getLocation(), statusCode, emptyResponse).inc();
                return response;
            } catch (Exception e) {
                requestFailureCount.labels(urlInfo.getLocation()).inc();
                throw e;
            } finally {
                timer.observeDuration();
            }
        }
    }

    static class SendAsync {
        static CompletableFuture<Response> intercept(@SuperCall Callable<CompletableFuture<Response>> zuper, @Argument(0) Request request) throws Exception {
            URLInfo urlInfo = new URLInfo(request.url());
            Histogram.Timer timer = requestDurationHistogram.labels(urlInfo.getLocation()).startTimer();
            requestStartedCount.labels(urlInfo.getLocation()).inc();
            return zuper.call()
                    .thenApply(response -> {
                        String statusCode = response == null ? "-1" : String.valueOf(response.statusCode());
                        String emptyResponse = response == null || !(response.body() != null && response.body().length > 0) ? "true" : "false";
                        requestCompletedCount.labels(urlInfo.getLocation(), statusCode, emptyResponse).inc();
                        timer.observeDuration();
                        return response;
                    })
                    .handle((response, throwable) -> {
                        if (throwable != null) {
                            requestFailureCount.labels(urlInfo.getLocation()).inc();
                            timer.observeDuration();
                        }
                        return response;
                    });
        }
    }

    public static class GetHandlerInterceptor {
        public static ExecutionContext intercept(@SuperCall Callable<ExecutionContext> zuper, @Argument(0) ExecutionContext context) throws Exception {
            try {
                return zuper.call();
            } catch (Exception e) {
                String url = context.state("PROMETHEUS_METRICS_REQUEST_URL");
                URLInfo urlInfo = new URLInfo(url);
                requestFailureCount.labels(urlInfo.getLocation()).inc();
                throw e;
            }
        }
    }

}
