package no.ssb.dc.core.handler;

import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.PositionObserver;
import no.ssb.dc.api.PositionProducer;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Sequence;
import no.ssb.dc.core.executor.BufferedReordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

@Handler(forClass = Sequence.class)
public class SequenceHandler extends AbstractNodeHandler<Sequence> {

    private final Logger LOG = LoggerFactory.getLogger(SequenceHandler.class);

    public SequenceHandler(Sequence node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        Response response = input.state(Response.class);
        List<?> splitToListItemList = Queries.from(node.splitToListQuery()).evaluateList(response.body());

        PageContext.Builder pageContextBuilder = input.state(PageContext.Builder.class);

        if (splitToListItemList.isEmpty()) {
            LOG.warn("Reached end of stream! No more elements from position: {}", pageContextBuilder.nextPositionVariableNames().stream().map(name -> name + "=" + input.variable(name)).collect(Collectors.toList()));
            throw new EndOfStreamException();
        }

        PositionProducer<?> positionProducer = input.state(PositionProducer.class);
        BufferedReordering<Position<?>> bufferedReordering = input.services().get(BufferedReordering.class);

        List<Position<?>> positionList = splitToListItemList.stream()
                .map(item -> Queries.from(node.expectedQuery()).evaluateStringLiteral(item))
                .map(positionProducer::produce)
                .collect(Collectors.toList());

        for (Position<?> position : positionList) {
            bufferedReordering.addExpected(position);
        }

        // only avail in scope if coming from paginate
        PositionObserver positionObserver = input.state(PositionObserver.class);
        if (positionObserver != null) {
            positionObserver.expected(positionList.size());
        }

        pageContextBuilder.expectedPositions(positionList);

        return ExecutionContext.empty();
    }

}
