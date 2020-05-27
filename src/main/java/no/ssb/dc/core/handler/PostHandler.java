package no.ssb.dc.core.handler;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;

@SuppressWarnings("unchecked")
@Handler(forClass = Post.class)
public class PostHandler extends AbstractOperationHandler<Post> {

    private static final Logger LOG = LoggerFactory.getLogger(PostHandler.class);

    public PostHandler(Post node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        int requestTimeout = beforeRequest(input);

        LOG.trace("============> vars: {}", input.variables());

        ConfigurationMap configurationMap = input.services().get(ConfigurationMap.class);
        ExecutionContext ctx = new ExecutionContext.Builder().variables(new LinkedHashMap<>(configurationMap.asMap())).build();

        ExpressionLanguage el = new ExpressionLanguage(ctx);
        String uid = (String) input.variable("uid");
        LOG.trace("uid: {}", uid);
        String u = el.evaluateExpressions(uid);
        LOG.trace("============> el: {}", u);

        byte[] bodyPublisher = new byte[0];
        Request.Builder requestBuilder = Request.newRequestBuilder().POST(bodyPublisher); // . timeout(Duration.ofSeconds(requestTimeout));

        Response response = doRequest(input, requestTimeout, requestBuilder);

        return handleResponse(input, response);
    }

}
