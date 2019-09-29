package no.ssb.dc.core.handler;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.CorrelationIds;
import no.ssb.dc.api.ExpressionLanguage;
import no.ssb.dc.api.Handler;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.content.HttpRequestInfo;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.http.Client;
import no.ssb.dc.api.http.Headers;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Get;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.core.executor.Executor;

import java.util.Map;

@SuppressWarnings("unchecked")
@Handler(forClass = Get.class)
public class GetHandler extends AbstractHandler<Get> {

    public GetHandler(Get node) {
        super(node);
    }

    static void copyInputHeadersToRequestBuilder(ExecutionContext input, Request.Builder requestBuilder) {
        Headers globalHeaders = input.state(Headers.class);
        if (globalHeaders != null) {
            globalHeaders.asMap().forEach((name, values) -> values.forEach(value -> requestBuilder.header(name, value)));
        }
    }

    private String evaluatedUrl(Map<String, Object> variables) {
        ExpressionLanguage el = new ExpressionLanguage(variables);
        return el.evaluateExpressions(node.url());
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        // prepare get request
        Request.Builder requestBuilder = Request.newRequestBuilder().GET();

        // prepare request headers
        copyInputHeadersToRequestBuilder(input, requestBuilder);

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

        HttpRequestInfo httpRequestInfo = new HttpRequestInfo(CorrelationIds.of(input), url, request.headers(), response.headers(), durationNanoSeconds);
        input.state(HttpRequestInfo.class, httpRequestInfo);

        // add page content
        boolean addPageContent = input.state(PaginateHandler.ADD_PAGE_CONTENT) != null && (Boolean) input.state(PaginateHandler.ADD_PAGE_CONTENT);

        if (addPageContent) {
            ConfigurationMap config = input.services().get(ConfigurationMap.class);
            ContentStore contentStore = input.services().get(ContentStore.class);
            contentStore.addPaginationDocument(config.get("namespace.default"),"page", response.body(), httpRequestInfo);
            input.releaseState(PaginateHandler.ADD_PAGE_CONTENT);
        }

        // create output
        ExecutionContext accumulated = ExecutionContext.empty();
        accumulated.state(Response.class, response);

        // handle step nodes
        for (Node step : node.steps()) {
            ExecutionContext stepInput = ExecutionContext.of(input).merge(accumulated);

            ExecutionContext stepOutput = Executor.execute(step, stepInput);
            accumulated.merge(stepOutput);
        }

        // return only variables declared in returnVariables
        ExecutionContext output = ExecutionContext.of(accumulated).merge(CorrelationIds.of(input).context());
        node.returnVariables().forEach(variableKey -> output.state(variableKey, accumulated.state(variableKey)));

        return output;
    }
}
