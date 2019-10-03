package no.ssb.dc.core.handler;

import no.ssb.dc.api.Processor;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Process;
import no.ssb.dc.api.services.ObjectCreator;

@Handler(forClass = Process.class)
public class ProcessHandler extends AbstractNodeHandler<Process> {

    public ProcessHandler(Process process) {
        super(process);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        Processor processInstance = ObjectCreator.newInstance(node.processorClass(), input.services());

        ExecutionContext processInput = ExecutionContext.of(input);
        processInput.state(Response.class, input.state(Response.class));

        ExecutionContext processorOutput = processInstance.process(processInput);

        if (node.requiredOutputs().stream().noneMatch(key -> processorOutput.variables().containsKey(key))) {
            throw new RuntimeException("A variable is missing!");
        }

        return processorOutput;
    }

}
