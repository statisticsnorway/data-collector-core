package no.ssb.dc.core.handler;

import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.node.Sequence;
import no.ssb.dc.core.executor.BufferedReordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Handler(forClass = Sequence.class)
public class SequenceHandler extends AbstractHandler<Sequence> {

    private final Logger LOG = LoggerFactory.getLogger(SequenceHandler.class);

    public SequenceHandler(Sequence node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        List<?> splitToListItemList = Queries.getItemList(node.splitToListQuery(), input);

        if (splitToListItemList.isEmpty()) {
            LOG.warn("Reached end of stream!");
            throw new EndOfStreamException();
        }

        BufferedReordering<Position<?>> bufferedReordering = input.services().get(BufferedReordering.class);
        for (Position<?> position : Queries.getPositionMap(node.expectedQuery(), splitToListItemList).keySet()) {
            bufferedReordering.addExpected(position);
        }

        return ExecutionContext.empty();
    }

}
