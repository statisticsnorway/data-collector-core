package no.ssb.dc.core.handler;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.error.ExecutionException;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.Operation;
import no.ssb.dc.api.node.Validator;
import no.ssb.dc.api.util.CommonUtils;
import no.ssb.dc.core.executor.Executor;
import no.ssb.dc.core.health.HealthWorkerMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AbstractOperationHandler<T extends Operation> extends AbstractNodeHandler<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractOperationHandler.class);

    public AbstractOperationHandler(T node) {
        super(node);
    }

    static void copyInputHeadersToRequestBuilder(ExecutionContext input, Request.Builder requestBuilder) {
        // todo fails here because there is a headers in both globalState and state, where state is read and contains an empty map
        Headers globalHeaders = input.state(Headers.class);
        if (globalHeaders != null) {
            globalHeaders.asMap().forEach((name, values) -> values.forEach(value -> requestBuilder.header(name, value)));
        }
    }

    static void copyNodeHeadersToRequestBuilder(Operation node, Request.Builder requestBuilder) {
        if (node.headers() == null) {
            return;
        }
        node.headers().asMap().forEach((name, values) -> values.forEach(value -> requestBuilder.header(name, value)));
    }

    private String evaluatedUrl(ExecutionContext context) {
        ExpressionLanguage el = new ExpressionLanguage(context);
        return el.evaluateExpressions(node.url());
    }

    int beforeRequest(ExecutionContext input) {
        // prepare get request
        ConfigurationMap configurationMap = input.services().get(ConfigurationMap.class);
        int requestTimeout = configurationMap != null ? Integer.parseInt(configurationMap.get("data.collector.http.request.timeout.seconds")) : 15;
        return requestTimeout;
    }

    Response doRequest(ExecutionContext input, int requestTimeout, Request.Builder requestBuilder) {
        // prepare request headers
        copyInputHeadersToRequestBuilder(input, requestBuilder);
        copyNodeHeadersToRequestBuilder(node, requestBuilder);

        // evaluate url with expressions
        String url = evaluatedUrl(input);
        requestBuilder.url(url);

        // Expose node.url() to ByteBuddy Agent
        input.state("PROMETHEUS_METRICS_REQUEST_URL", url);

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
        //CorrelationIds correlationIdBeforeChildren = CorrelationIds.of(input);
        HttpRequestInfo httpRequestInfo = new HttpRequestInfo(CorrelationIds.create(input), url, response.statusCode(), request.headers(), response.headers(), durationMillisSeconds);
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
            // TODO consider to expand with EL and Query
            String positionVariable = input.state(PaginateHandler.ADD_PAGE_CONTENT_TO_POSITION);
            if (positionVariable == null || positionVariable.isBlank()) {
                throw new ExecutionException(String.format("The position is undefined for %s.addPageContent(positionVariable)!", node.id()));
            }
            Object position = input.variable(positionVariable);
            ;
            if (position == null) {
                throw new ExecutionException(String.format("Unable to resolve position for function %s.addPageContent(%s)!", node.id(), positionVariable));
            }
            contentStore.addPaginationDocument(topicName, position.toString(), "page", response.body(), httpRequestInfo);
            input.releaseState(PaginateHandler.ADD_PAGE_CONTENT);
        }
        return response;
    }

    ExecutionContext handleResponse(ExecutionContext input, Response response) {
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
        //CorrelationIds correlationIdsAfterChildren = CorrelationIds.of(input);
        ExecutionContext output = ExecutionContext.of(accumulated); //.merge(correlationIdsAfterChildren.context());
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
                HealthWorkerMonitor monitor = context.services().get(HealthWorkerMonitor.class);
                if (monitor != null) {
                    monitor.request().incrementRequestRetryOnFailureCount();
                }
                LOG.error("Request error occurred - retrying {} of {}: {}. Cause: {}", retry + 1, retryCount, request.url(), CommonUtils.captureStackTrace(e));
                nap(150);
            }
        }
        return response;
    }

    private Response executeRequest(ExecutionContext context, Client client, Request request, int requestTimeout) {
        AtomicReference<Throwable> failureCause = new AtomicReference<>();

        CompletableFuture<Response> requestFuture = client.sendAsync(request)
                .exceptionally(throwable -> {
                    failureCause.compareAndSet(null, throwable);

                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }
                    throw new ExecutionException(throwable);
                });

        try {
            Response response = requestFuture.get(requestTimeout, TimeUnit.SECONDS);

            // fire validation handlers
            if (response != null) {
                for (Validator responseValidator : node.responseValidators()) {
                    Executor.execute(responseValidator, ExecutionContext.of(context).state(Response.class, response));
                }
            }

            if (failureCause.get() != null) {
                // TODO should exception be rethrown
                LOG.error("Captured completable exception: {}", CommonUtils.captureStackTrace(failureCause.get()));
            }

            return response;

        } catch (Exception e) {
            failureCause.compareAndSet(null, e);

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

}
