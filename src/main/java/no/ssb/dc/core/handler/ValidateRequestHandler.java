package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.ValidateRequest;

@Handler(forClass = ValidateRequest.class)
public class ValidateRequestHandler extends AbstractHandler<ValidateRequest>  {

    public ValidateRequestHandler(ValidateRequest node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        return ExecutionContext.empty();
    }
}
