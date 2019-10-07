package no.ssb.dc.core.handler;

import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.PositionProducer;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Get;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.Validator;
import no.ssb.dc.core.executor.Executor;

import java.util.Map;

@SuppressWarnings("unchecked")
@Handler(forClass = Get.class)
public class GetHandler extends AbstractNodeHandler<Get> {

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

    private String evaluatedUrl(Map<String, Object> variables) {
        ExpressionLanguage el = new ExpressionLanguage(variables);
        return el.evaluateExpressions(node.url());
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        // prepare get request
        Request.Builder requestBuilder = Request.newRequestBuilder().GET();

        // prepare request headers
        copyInputHeadersToRequestBuilder(input, requestBuilder);
        copyNodeHeadersToRequestBuilder(node, requestBuilder);

        // evaluate url with expressions
        String url = evaluatedUrl(input.variables());
        requestBuilder.url(url);

        // execute http get
        Client client = input.services().get(Client.class);
        Request request = requestBuilder.build();
        long currentNanoSeconds = System.nanoTime();
        Response response = client.send(request);
        long futureNanoSeconds = System.nanoTime();
        long durationNanoSeconds = futureNanoSeconds - currentNanoSeconds;

        // fire validation handlers
        for (Validator responseValidator : node.responseValidators()) {
            Executor.execute(responseValidator, ExecutionContext.of(input).state(Response.class, response));
        }

        // prepare http-request-info used by content producer
        HttpRequestInfo httpRequestInfo = new HttpRequestInfo(CorrelationIds.of(input), url, request.headers(), response.headers(), durationNanoSeconds);
        input.state(HttpRequestInfo.class, httpRequestInfo);

        // make sure we have a position producer
        input.state(PositionProducer.class, node.createOrGetPositionProducer());

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
}
