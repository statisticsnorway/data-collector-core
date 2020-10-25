package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Get;
import no.ssb.dc.api.node.JsonToken;
import no.ssb.dc.api.node.Node;
import no.ssb.dc.api.node.Sequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@SuppressWarnings("unchecked")
@Handler(forClass = Get.class)
public class GetHandler extends OperationHandler<Get> {

    private static final Logger LOG = LoggerFactory.getLogger(GetHandler.class);

    public GetHandler(Get node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        int requestTimeout = beforeRequest(input);

        // detect Sequence jsonToken-query and mark OperationHandler to use TempFileBodyHandler
        // OperationHandler.executeRequest will resolve BodyHandler form input state context
        Optional<Sequence> sequenceNode = Optional.ofNullable(findSequenceNode());
        if (sequenceNode.isPresent()) {
            if (sequenceNode.get().splitToListQuery() instanceof JsonToken) {
                LOG.trace("Use TempFileBodyHandler on OperationNode: {}", node.id());
                input.state(no.ssb.dc.api.http.BodyHandler.class, TempFileBodyHandler.ofFile());
            }
        }

        Request.Builder requestBuilder = Request.newRequestBuilder().GET(); // . timeout(Duration.ofSeconds(requestTimeout));

        Response response = doRequest(input, requestTimeout, requestBuilder);

        return handleResponse(input, response);
    }

    private Sequence findSequenceNode() {
        for (Node node : node.steps()) {
            if (node instanceof Sequence) {
                return (Sequence) node;
            }
        }
        return null;
    }

}