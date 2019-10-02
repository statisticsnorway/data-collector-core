package no.ssb.dc.core.handler;

import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.ValidateResponse;

@Handler(forClass = ValidateResponse.class)
public class ValidateRequestHandler extends AbstractHandler<ValidateResponse>  {

    public ValidateRequestHandler(ValidateResponse node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        return ExecutionContext.empty();
    }
}
