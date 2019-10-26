package no.ssb.dc.core.health;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import no.ssb.dc.api.content.HealthContentStreamMonitor;
import no.ssb.dc.api.health.HealthResourceUtils;
import no.ssb.dc.core.executor.WorkerStatus;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    final Request request = new Request();
    final ContentStore contentStore = new ContentStore();
    final Map<String, Object> threadPoolInfo = new LinkedHashMap<>();

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
                threadPoolInfo
        );
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
        final Map<String, List<String>> requestHeaders = new LinkedHashMap<>();

        final AtomicInteger prefetchThresholdRef = new AtomicInteger(0);
        final AtomicLong prefetchCountRef = new AtomicLong(0);
        final AtomicLong totalExpectedCountRef = new AtomicLong(0);
        final AtomicLong totalCompletedCountRef = new AtomicLong(0);

        final AtomicLong completedRequestCountRef = new AtomicLong(0);
        final AtomicLong lastRequestDurationNanoSecondsRef = new AtomicLong(0);
        final AtomicLong requestDurationNanoSecondsRef = new AtomicLong(0);
        final AtomicLong requestRetryOnFailureCountRef = new AtomicLong(0);


        public void setHeaders(Map<String, List<String>> requestHeaders) {
            this.requestHeaders.putAll(requestHeaders);
        }

        public void incrementCompletedRequestCount() {
            completedRequestCountRef.incrementAndGet();
        }

        public void updateLastRequestDurationNanoSeconds(long durationNanoSeconds) {
            lastRequestDurationNanoSecondsRef.set(durationNanoSeconds);
        }

        public void addRequestDurationNanoSeconds(long durationNanoSeconds) {
            requestDurationNanoSecondsRef.addAndGet(durationNanoSeconds);
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
            float avgRequestDurationNannos = HealthResourceUtils.divide(requestDurationNanoSecondsRef.get(), completedRequestCountRef.get());
            float averageRequestDurationMillis = (avgRequestDurationNannos / 100_000);
            return new RequestInfo(
                    requestHeaders,
                    completedRequestCountRef.get(),
                    lastRequestDurationNanoSecondsRef.get() / 100_000,
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
        final AtomicReference<String> lastPositionRef = new AtomicReference<>();
        final AtomicReference<HealthContentStreamMonitor> contentStreamMonitorRef = new AtomicReference<>();
        String topic;

        public void setTopic(String topic) {
            this.topic = topic;
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
                   Map<String, Object> threadPoolInfo
        ) {
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
        @JsonProperty("request-headers") public final Map<String, List<String>> requestHeaders;
        @JsonProperty("request-count") public final Long requestCount;
        @JsonProperty("last-request-duration-millis") public final Long lastRequestDurationMillis;
        @JsonProperty("avg-request-duration-millis") public final Integer averageRequestDurationMillis;
        @JsonProperty("retry-on-failure-count") public final Long retryOnFailureCount;
        @JsonProperty("prefetch-threshold") public final int prefetchThreshold;
        @JsonProperty("prefetch-count") public final Long prefetchCount;
        @JsonProperty("expected-count") public final Long expectedCount;
        @JsonProperty("completed-count") public final Long completedCount;

        RequestInfo(Map<String, List<String>> requestHeaders, Long requestCount, Long lastRequestDurationMillis, Integer averageRequestDurationMillis, Long retryOnFailureCount, int prefetchThreshold
                , Long prefetchCount, Long expectedCount, Long completedCount) {
            this.requestHeaders = requestHeaders;
            this.requestCount = requestCount;
            this.lastRequestDurationMillis = lastRequestDurationMillis;
            this.averageRequestDurationMillis = averageRequestDurationMillis;
            this.retryOnFailureCount = retryOnFailureCount;
            this.prefetchThreshold = prefetchThreshold;
            this.prefetchCount = prefetchCount;
            this.expectedCount = expectedCount;
            this.completedCount = completedCount;
        }
    }

    @JsonPropertyOrder({"topic", "last-position"})
    @SuppressWarnings("WeakerAccess")
    public static class ContentStoreInfo {
        @JsonProperty("topic") public final String topic;
        @JsonProperty("last-position") public final String lastPosition;
        @JsonUnwrapped public final HealthContentStreamMonitor.ContentStreamInfo contentStream;

        ContentStoreInfo(String topic, String lastPosition, HealthContentStreamMonitor.ContentStreamInfo contentStream) {
            this.topic = topic;
            this.lastPosition = lastPosition;
            this.contentStream = contentStream;
        }

        @JsonIgnore
        public String getTopic() {
            return topic;
        }
    }

}
