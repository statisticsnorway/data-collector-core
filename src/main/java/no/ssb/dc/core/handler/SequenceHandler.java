package no.ssb.dc.core.handler;

import no.ssb.dc.api.Position;
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
public class SequenceHandler extends AbstractHandler<Sequence> {

    private final Logger LOG = LoggerFactory.getLogger(SequenceHandler.class);

    public SequenceHandler(Sequence node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        Response response = input.state(Response.class);
        List<?> splitToListItemList = Queries.evaluate(node.splitToListQuery()).queryList(response.body());

        if (splitToListItemList.isEmpty()) {
            LOG.warn("Reached end of stream!");
            throw new EndOfStreamException();
        }

        BufferedReordering<Position<?>> bufferedReordering = input.services().get(BufferedReordering.class);

        List<Position<?>> positionList = splitToListItemList.stream()
                .map(item -> Queries.evaluate(node.expectedQuery()).queryStringLiteral(item))
                .map(Position::new)
                .collect(Collectors.toList());

        for (Position<?> position : positionList) {
            bufferedReordering.addExpected(position);
        }

        return ExecutionContext.empty();
    }

}
