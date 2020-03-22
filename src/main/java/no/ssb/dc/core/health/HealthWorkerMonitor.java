package no.ssb.dc.core.health;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.ssb.dc.api.content.HealthContentStreamMonitor;
import no.ssb.dc.api.health.HealthResourceUtils;
import no.ssb.dc.core.executor.WorkerStatus;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class HealthWorkerMonitor {

    final AtomicReference<WorkerStatus> statusRef = new AtomicReference<>();
    final AtomicReference<String> specificationIdRef = new AtomicReference<>();
    final AtomicReference<String> nameRef = new AtomicReference<>();
    final AtomicReference<String> startFunctionRef = new AtomicReference<>();
    final AtomicReference<String> startFunctionIdRef = new AtomicReference<>();
    final AtomicLong startedRef = new AtomicLong(0);
    final AtomicLong endedRef = new AtomicLong(0);
    final AtomicReference<String> failureCauseRef = new AtomicReference<>();
    final Security security = new Security();
    final Request request = new Request(startedRef::get);
    final ContentStore contentStore = new ContentStore();
    final Map<String, Object> threadPoolInfo = new LinkedHashMap<>();
    final AtomicReference<ObjectNode> threadDumpNodeRef = new AtomicReference<>();

    public WorkerStatus status() {
        return statusRef.get();
    }

    public void setStatus(WorkerStatus status) {
        statusRef.set(status);
    }

    public void setSpecificationId(String specificationId) {
        specificationIdRef.set(specificationId);
    }

    public void setName(String name) {
        this.nameRef.set(name);
    }

    public void setStartFunction(String function) {
        this.startFunctionRef.set(function);
    }

    public void setStartFunctionId(String functionId) {
        this.startFunctionIdRef.set(functionId);
    }

    public void setStartedTimestamp() {
        startedRef.set(Instant.now().toEpochMilli());
    }

    public void setEndedTimestamp() {
        endedRef.set(Instant.now().toEpochMilli());
    }

    public void setFailureCause(String failureCause) {
        failureCauseRef.set(failureCause);
    }

    public void setThreadDumpNode(ObjectNode threadDumpNode) {
        threadDumpNodeRef.set(threadDumpNode);
    }

    public Security security() {
        return security;
    }

    public Request request() {
        return request;
    }

    public ContentStore contentStream() {
        return contentStore;
    }

    public void setThreadPoolInfo(Map<String, Object> threadPoolInfo) {
        this.threadPoolInfo.putAll(threadPoolInfo);
    }

    public WorkerInfo build() {
        return new WorkerInfo(
                statusRef.get(),
                specificationIdRef.get(),
                nameRef.get(),
                startedRef.get() == 0L ? null : Instant.ofEpochMilli(startedRef.get()).toString(),
                endedRef.get() == 0L ? null : Instant.ofEpochMilli(endedRef.get()).toString(),
                startedRef.get() == 0L ? null : HealthResourceUtils.durationAsString(startedRef.get()),
                startFunctionRef.get(),
                startFunctionIdRef.get(),
                failureCauseRef.get(),
                security.build(),
                request.build(),
                contentStore.build(),
                threadPoolInfo,
                threadDumpNodeRef.get());
    }

    public static class Security {
        String sslBundleName;

        public void setSslBundleName(String sslBundleName) {
            this.sslBundleName = sslBundleName;
        }

        SecurityInfo build() {
            return new SecurityInfo(sslBundleName);
        }
    }

    public static class Request {
        final AtomicInteger httpClientTimeoutSecondsRef = new AtomicInteger();
        final AtomicInteger httpRequestTimeoutSecondsRef = new AtomicInteger();

        final Map<String, List<String>> requestHeaders = new LinkedHashMap<>();

        final AtomicInteger prefetchThresholdRef = new AtomicInteger(0);
        final AtomicLong prefetchCountRef = new AtomicLong(0);
        final AtomicLong totalExpectedCountRef = new AtomicLong(0);
        final AtomicLong totalCompletedCountRef = new AtomicLong(0);

        final AtomicLong completedRequestCountRef = new AtomicLong(0);
        final AtomicLong lastRequestDurationMilliSecondsRef = new AtomicLong(0);
        final AtomicLong requestDurationMilliSecondsRef = new AtomicLong(0);
        final AtomicLong requestRetryOnFailureCountRef = new AtomicLong(0);

        final Supplier<Long> startedInMillisSupplier;

        public Request(Supplier<Long> startedInMillisSupplier) {
            this.startedInMillisSupplier = startedInMillisSupplier;
        }

        public void setHttpClientTimeoutSeconds(int timeout) {
            httpClientTimeoutSecondsRef.set(timeout);
        }

        public void setHttpRequestTimeoutSeconds(int timeout) {
            httpRequestTimeoutSecondsRef.set(timeout);
        }

        public void setHeaders(Map<String, List<String>> requestHeaders) {
            this.requestHeaders.putAll(requestHeaders);
        }

        public void incrementCompletedRequestCount() {
            completedRequestCountRef.incrementAndGet();
        }

        public void updateLastRequestDurationMillisSeconds(long durationMilliSeconds) {
            lastRequestDurationMilliSecondsRef.set(durationMilliSeconds);
        }

        public void addRequestDurationMillisSeconds(long durationMilliSeconds) {
            requestDurationMilliSecondsRef.addAndGet(durationMilliSeconds);
        }

        public void incrementRequestRetryOnFailureCount() {
            requestRetryOnFailureCountRef.incrementAndGet();
        }

        public void setPrefetchThreshold(int threshold) {
            prefetchThresholdRef.set(threshold);
        }

        public void incrementPrefetchCount() {
            prefetchCountRef.incrementAndGet();
        }

        public void decrementPrefetchCount() {
            prefetchCountRef.decrementAndGet();
        }

        public void updateTotalExpectedCount(Integer expectedCount) {
            totalExpectedCountRef.addAndGet(expectedCount);
        }

        public void updateTotalCompletedCount(Integer completedCount) {
            totalCompletedCountRef.addAndGet(completedCount);
        }

        RequestInfo build() {
            long now = System.currentTimeMillis();
            Float averageRequestPerSecond = HealthResourceUtils.divide(completedRequestCountRef.get(), (now - startedInMillisSupplier.get()) / 1000);
            DecimalFormat df = new DecimalFormat("#.##");
            df.setRoundingMode(RoundingMode.UP);
            averageRequestPerSecond = Float.parseFloat(df.format(averageRequestPerSecond));

            float avgRequestDurationMillis = HealthResourceUtils.divide(requestDurationMilliSecondsRef.get(), completedRequestCountRef.get());
            float averageRequestDurationMillis = (avgRequestDurationMillis);

            return new RequestInfo(
                    httpClientTimeoutSecondsRef.get(),
                    httpRequestTimeoutSecondsRef.get(),
                    requestHeaders,
                    completedRequestCountRef.get(),
                    averageRequestPerSecond,
                    lastRequestDurationMilliSecondsRef.get(),
                    Math.round(averageRequestDurationMillis),
                    requestRetryOnFailureCountRef.get(),
                    prefetchThresholdRef.get(),
                    prefetchCountRef.get(),
                    totalExpectedCountRef.get(),
                    totalCompletedCountRef.get()
            );
        }
    }

    public static class ContentStore {
        final AtomicBoolean hasNotSetStartPositionRef = new AtomicBoolean(true);
        final AtomicReference<String> startPositionRef = new AtomicReference<>();
        final AtomicReference<String> lastPositionRef = new AtomicReference<>();
        final AtomicReference<HealthContentStreamMonitor> contentStreamMonitorRef = new AtomicReference<>();
        String topic;

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public boolean hasNotSetStartPosition() {
            return hasNotSetStartPositionRef.get();
        }

        public void setStartPosition(String startPosition) {
            if (startPositionRef.compareAndSet(null, startPositionRef.get())) {
                hasNotSetStartPositionRef.set(false);
                startPositionRef.set(startPosition);
            }
        }

        public void setLastPosition(String lastPosition) {
            lastPositionRef.set(lastPosition);
        }

        HealthContentStreamMonitor monitor() {
            return contentStreamMonitorRef.get();
        }

        public void setMonitor(HealthContentStreamMonitor monitor) {
            contentStreamMonitorRef.set(monitor);
        }

        ContentStoreInfo build() {
            return new ContentStoreInfo(
                    topic,
                    startPositionRef.get(),
                    lastPositionRef.get(),
                    contentStreamMonitorRef.get().build()
            );
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @SuppressWarnings("WeakerAccess")
    public static class WorkerInfo {
        @JsonIgnore public final WorkerStatus status;
        @JsonProperty("specificationId") public final String specificationId;
        @JsonProperty("name") public final String name;
        @JsonProperty("started") public final String started;
        @JsonProperty("ended") public final String ended;
        @JsonProperty("duration") public final String duration;
        @JsonProperty("start-function") public final String startFunction;
        @JsonProperty("start-function-id") public final String startFunctionId;
        @JsonProperty("failure-cause") public final String failureCause;
        @JsonProperty("security") public final SecurityInfo securityInfo;
        @JsonProperty("request-info") public final RequestInfo requestInfo;
        @JsonProperty("content-stream") public final ContentStoreInfo contentStoreInfo;
        @JsonProperty("thread-pool") public final Map<String, Object> threadPoolInfo;
        @JsonProperty("thread-dump") public final ObjectNode threadDumpNode;

        WorkerInfo(WorkerStatus status,
                   String specificationId,
                   String name,
                   String started,
                   String ended,
                   String duration,
                   String startFunction,
                   String startFunctionId,
                   String failureCause,
                   SecurityInfo securityInfo,
                   RequestInfo requestInfo,
                   ContentStoreInfo contentStoreInfo,
                   Map<String, Object> threadPoolInfo,
                   ObjectNode threadDumpNode) {
            this.status = status;
            this.specificationId = specificationId;
            this.name = name;
            this.started = started;
            this.ended = ended;
            this.duration = duration;
            this.startFunction = startFunction;
            this.startFunctionId = startFunctionId;
            this.failureCause = failureCause;
            this.securityInfo = securityInfo;
            this.requestInfo = requestInfo;
            this.contentStoreInfo = contentStoreInfo;
            this.threadPoolInfo = threadPoolInfo;
            this.threadDumpNode = threadDumpNode;
        }

        @JsonIgnore
        public String getName() {
            return name;
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class SecurityInfo {
        @JsonProperty("ssl-bundle-name") public final String sslBundleName;

        SecurityInfo(String sslBundleName) {
            this.sslBundleName = sslBundleName;
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class RequestInfo {
        @JsonProperty("http-client-timeout-seconds") public final Integer httpClientTimeoutSeconds;
        @JsonProperty("http-request-timeout-seconds") public final Integer httpRequestTimeoutSeconds;
        @JsonProperty("request-headers") public final Map<String, List<String>> requestHeaders;
        @JsonProperty("request-count") public final Long requestCount;
        @JsonProperty("avg-requests-per-second") public final Float averageRequestPerSecond;
        @JsonProperty("last-request-duration-millis") public final Long lastRequestDurationMillis;
        @JsonProperty("avg-request-duration-millis") public final Integer averageRequestDurationMillis;
        @JsonProperty("retry-on-failure-count") public final Long retryOnFailureCount;
        @JsonProperty("prefetch-threshold") public final int prefetchThreshold;
        @JsonProperty("prefetch-count") public final Long prefetchCount;
        @JsonProperty("expected-count") public final Long expectedCount;
        @JsonProperty("completed-count") public final Long completedCount;

        RequestInfo(Integer httpClientTimeoutSeconds, Integer httpRequestTimeoutSeconds, Map<String, List<String>> requestHeaders, Long requestCount, Float averageRequestPerSecond, Long lastRequestDurationMillis, Integer averageRequestDurationMillis, Long retryOnFailureCount, int prefetchThreshold
                , Long prefetchCount, Long expectedCount, Long completedCount) {
            this.httpClientTimeoutSeconds = httpClientTimeoutSeconds;
            this.httpRequestTimeoutSeconds = httpRequestTimeoutSeconds;
            this.requestHeaders = requestHeaders;
            this.requestCount = requestCount;
            this.averageRequestPerSecond = averageRequestPerSecond;
            this.lastRequestDurationMillis = lastRequestDurationMillis;
            this.averageRequestDurationMillis = averageRequestDurationMillis;
            this.retryOnFailureCount = retryOnFailureCount;
            this.prefetchThreshold = prefetchThreshold;
            this.prefetchCount = prefetchCount;
            this.expectedCount = expectedCount;
            this.completedCount = completedCount;
        }
    }

    @JsonPropertyOrder({"topic", "start-position", "last-position"})
    @SuppressWarnings("WeakerAccess")
    public static class ContentStoreInfo {
        @JsonProperty("topic") public final String topic;
        @JsonProperty("start-position") public final String startPosition;
        @JsonProperty("last-position") public final String lastPosition;
        @JsonUnwrapped public final HealthContentStreamMonitor.ContentStreamInfo contentStream;

        ContentStoreInfo(String topic, String startPosition, String lastPosition, HealthContentStreamMonitor.ContentStreamInfo contentStream) {
            this.topic = topic;
            this.startPosition = startPosition;
            this.lastPosition = lastPosition;
            this.contentStream = contentStream;
        }

        @JsonIgnore
        public String getTopic() {
            return topic;
        }
    }

}
