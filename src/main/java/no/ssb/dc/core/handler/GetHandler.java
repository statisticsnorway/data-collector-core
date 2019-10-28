package no.ssb.dc.core.handler;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.error.ExecutionException;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.HttpStatusCode;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Get;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.Validator;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.health.HealthWorkerMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("unchecked")
@Handler(forClass = Get.class)
public class GetHandler extends AbstractNodeHandler<Get> {

    private static final Logger LOG = LoggerFactory.getLogger(GetHandler.class);

    public GetHandler(Get node) {
        super(node);
    }

    static void copyInputHeadersToRequestBuilder(ExecutionContext input, Request.Builder requestBuilder) {
        // todo fails here because there is a headers in both globalState and state, where state is read and contains an empty map
        Headers globalHeaders = input.state(Headers.class);
        if (globalHeaders != null) {
            globalHeaders.asMap().forEach((name, values) -> values.forEach(value -> requestBuilder.header(name, value)));
        }
    }

    static void copyNodeHeadersToRequestBuilder(Get node, Request.Builder requestBuilder) {
        if (node.headers() == null) {
            return;
        }
        node.headers().asMap().forEach((name, values) -> values.forEach(value -> requestBuilder.header(name, value)));
    }

    private String evaluatedUrl(ExecutionContext context) {
        ExpressionLanguage el = new ExpressionLanguage(context);
        return el.evaluateExpressions(node.url());
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        // prepare get request
        ConfigurationMap configurationMap = input.services().get(ConfigurationMap.class);
        int requestTimeout = configurationMap != null ? Integer.parseInt(configurationMap.get("data.collector.http.request.timeout.seconds")) : 15;
        Request.Builder requestBuilder = Request.newRequestBuilder().GET().timeout(Duration.ofSeconds(requestTimeout));

        // prepare request headers
        copyInputHeadersToRequestBuilder(input, requestBuilder);
        copyNodeHeadersToRequestBuilder(node, requestBuilder);

        // evaluate url with expressions
        String url = evaluatedUrl(input);
        requestBuilder.url(url);

        // execute http get
        Client client = input.services().get(Client.class);
        Request request = requestBuilder.build();
        long currentMillisSeconds = System.currentTimeMillis();
        Response response = sendAndRetryRequestOnError(input, client, request, requestTimeout, 3);
        long futureMillisSeconds = System.currentTimeMillis();
        long durationMillisSeconds = futureMillisSeconds - currentMillisSeconds;

        HealthWorkerMonitor monitor = input.services().get(HealthWorkerMonitor.class);
        if (monitor != null) {
            monitor.request().incrementCompletedRequestCount();
            monitor.request().updateLastRequestDurationMillisSeconds(durationMillisSeconds);
            monitor.request().addRequestDurationMillisSeconds(durationMillisSeconds);
        }

        if (response == null) {
            throw new IllegalStateException("No response received. An unhandled error occurred: " + request.url());
        }

        // prepare http-request-info used by content producer
        HttpRequestInfo httpRequestInfo = new HttpRequestInfo(CorrelationIds.of(input), url, request.headers(), response.headers(), durationMillisSeconds);
        input.state(HttpRequestInfo.class, httpRequestInfo);

        // add page content
        boolean addPageContent = input.state(PaginateHandler.ADD_PAGE_CONTENT) != null && (Boolean) input.state(PaginateHandler.ADD_PAGE_CONTENT);

        // TODO end-of-stream is determined in Sequence. An unintended empty page will be added to content store
        if (addPageContent) {
            ContentStore contentStore = input.services().get(ContentStore.class);
            String topicName = node.configurations().flowContext().topic();
            if (topicName == null) {
                throw new IllegalStateException("Unable to resolve topic!");
            }
            contentStore.addPaginationDocument(topicName, "page", response.body(), httpRequestInfo);
            input.releaseState(PaginateHandler.ADD_PAGE_CONTENT);
        }

        // create output
        ExecutionContext accumulated = ExecutionContext.empty();
        accumulated.state(Response.class, response);

        PageContext.Builder pageContextBuilder = new PageContext.Builder();
        pageContextBuilder.addNextPositionVariableNames(node.returnVariables());

        // handle step nodes
        for (Node step : node.steps()) {
            ExecutionContext stepInput = ExecutionContext.of(input).merge(accumulated);
            stepInput.state(PageContext.Builder.class, pageContextBuilder);

            ExecutionContext stepOutput = Executor.execute(step, stepInput);
            accumulated.merge(stepOutput);
        }

        // return only variables declared in returnVariables
        ExecutionContext output = ExecutionContext.of(accumulated).merge(CorrelationIds.of(input).context());
        node.returnVariables().forEach(variableKey -> output.state(variableKey, accumulated.state(variableKey)));

        return output.state(PageContext.class, accumulated.state(PageContext.class));
    }

    private Response sendAndRetryRequestOnError(ExecutionContext context, Client client, Request request, int requestTimeout, int retryCount) {
        Response response = null;
        for (int retry = 0; retry < retryCount; retry++) {
            try {
                response = executeRequest(context, client, request, requestTimeout);
                break;
            } catch (Exception e) {
                if (retry == retryCount - 1) {
                    throw new ExecutionException(e);
                }

                HealthWorkerMonitor monitor = context.services().get(HealthWorkerMonitor.class);
                if (monitor != null) {
                    monitor.request().incrementRequestRetryOnFailureCount();
                }
                LOG.error("Request error occurred: {}. Retrying {} of {}. Cause: {}", request.url(), retry + 1, retryCount, CommonUtils.captureStackTrace(e));
                nap(150);
            }
        }
        return response;
    }

    private Response executeRequest(ExecutionContext context, Client client, Request request, int requestTimeout) {
        AtomicReference<Throwable> failureCause = new AtomicReference<>();

        CompletableFuture<Response> requestFuture = client.sendAsync(request)
//                .completeOnTimeout(new TimeoutResponse(request), requestTimeout, TimeUnit.SECONDS)
                .exceptionally(throwable -> {
                    if (failureCause.compareAndSet(null, throwable)) {
                        //LOG.error("Unable to store throwable in failedException, already set. Current exception: {}", CommonUtils.captureStackTrace(throwable));
                    }
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }
                    throw new ExecutionException(throwable);
                });

        try {
            Response response = requestFuture.join();

        /*
        if (response.statusCode() == HttpStatusCode.HTTP_CLIENT_TIMEOUT.statusCode()) {
            HttpStatusCode failedStatus = HttpStatusCode.HTTP_CLIENT_TIMEOUT;
            String message = String.format("Error dealing with response: %s [%s] %s%n", request.url(), failedStatus.statusCode(), failedStatus.reason());
            if (failureCause.compareAndSet(null, new TimeoutException(message))) {
                LOG.error("Unable to store throwable in failedException, already set. {}", message);
            }
        }
        */


            // fire validation handlers
            if (response != null) {
                for (Validator responseValidator : node.responseValidators()) {
                    Executor.execute(responseValidator, ExecutionContext.of(context).state(Response.class, response));
                }
            }

            return response;

        } catch (Exception e) {
            if (failureCause.compareAndSet(null, e)) {
                //LOG.error("Unable to store throwable in failedException, already set. Current exception: {}", CommonUtils.captureStackTrace(e));
            }
            LOG.error("HttpRequest failureCause: {}\n{}", request.url(), CommonUtils.captureStackTrace(failureCause.get()));
            if (failureCause.get() instanceof RuntimeException) {
                throw (RuntimeException) failureCause.get();
            }
            if (failureCause.get() instanceof Error) {
                throw (Error) failureCause.get();
            }
            throw new ExecutionException(failureCause.get());
        }
    }

    private void nap(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    static class TimeoutResponse implements Response {

        private final Request request;

        TimeoutResponse(Request request) {
            this.request = request;
        }

        @Override
        public String url() {
            return request.url();
        }

        @Override
        public Headers headers() {
            return new Headers(new LinkedHashMap<>());
        }

        @Override
        public int statusCode() {
            return HttpStatusCode.HTTP_CLIENT_TIMEOUT.statusCode();
        }

        @Override
        public byte[] body() {
            return new byte[0];
        }

        @Override
        public Optional<Response> previousResponse() {
            return Optional.empty();
        }
    }
}