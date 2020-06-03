package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.BodyPublisher;
import no.ssb.dc.api.node.Post;
import no.ssb.dc.core.executor.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.concurrent.Flow;

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

        Request.Builder requestBuilder = Request.newRequestBuilder();

        Flow.Publisher<ByteBuffer> byteBufferPublisher;
        if (node.bodyPublisher() != null) {
            ExecutionContext context = ExecutionContext.of(input).state(Request.Builder.class, requestBuilder);
            ExecutionContext output = Executor.execute(node.bodyPublisher(), context);
            byteBufferPublisher = output.state(BodyPublisher.BODY_PUBLISHER_RESULT);
        } else {
            byteBufferPublisher = HttpRequest.BodyPublishers.noBody();
        }

        requestBuilder.POST(byteBufferPublisher);

        Response response = doRequest(input, requestTimeout, requestBuilder);

        return handleResponse(input, response);
    }

}
