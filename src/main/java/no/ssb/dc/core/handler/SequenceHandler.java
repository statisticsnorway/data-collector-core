package no.ssb.dc.core.handler;

import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.PositionObserver;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.handler.QueryFeature;
import no.ssb.dc.api.http.BodyHandler;
import no.ssb.dc.api.http.Response;
import no.ssb.dc.api.node.Sequence;
import no.ssb.dc.core.executor.BufferedReordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
        BufferedReordering<String> bufferedReordering = input.services().get(BufferedReordering.class);
        PageContext.Builder pageContextBuilder = input.state(PageContext.Builder.class);

        List<String> positionList = new ArrayList<>();

        if (response.bodyHandler().isPresent()) {
            // split using sequential token deserializer
            doTokenizerQuery(input, response, positionList, bufferedReordering);

        } else {
            // split by query
            doSplitQuery(input, response, positionList, bufferedReordering);
        }

        if (positionList.isEmpty()) {
            LOG.warn("Reached end of stream! No more elements from position: {}", pageContextBuilder.nextPositionVariableNames().stream().map(name -> name + "=" + input.variable(name)).collect(Collectors.toList()));
            throw new EndOfStreamException();
        }

        // only avail in scope if coming from paginate
        PositionObserver positionObserver = input.state(PositionObserver.class);
        if (positionObserver != null) {
            positionObserver.expected(positionList.size());
        }

        pageContextBuilder.expectedPositions(positionList);

        return ExecutionContext.empty();
    }

    void doTokenizerQuery(ExecutionContext input, Response response, List<String> positionList, BufferedReordering<String> bufferedReordering) {
        try {
            final QueryFeature expectedQuery = Queries.from(input, node.expectedQuery());
            final DocumentParserFeature parser = Queries.parserFor(node.splitToListQuery().getClass());
            final BodyHandler<Path> fileBodyHandler = response.<Path>bodyHandler().orElseThrow();

            try (FileInputStream fis = new FileInputStream(fileBodyHandler.body().toFile())) {
                AtomicLong counter = new AtomicLong();
                parser.tokenDeserializer(fis, entry -> {
                    String position = expectedQuery.evaluateStringLiteral(entry);
                    positionList.add(position);
                    bufferedReordering.addExpected(position);
                    if (counter.incrementAndGet() % 50000 == 0) {
                        LOG.trace("Sequence count: {}", counter.get());
                    }
                });
                LOG.info("Sequence count: {}", counter.get());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void doSplitQuery(ExecutionContext input, Response response, List<String> positionList, BufferedReordering<String> bufferedReordering) {
        final List<?> splitToListItemList = Queries.from(input, node.splitToListQuery()).evaluateList(response.body());
        final QueryFeature queryFeature = Queries.from(input, node.expectedQuery());

        for (Object item : splitToListItemList) {
            String position = queryFeature.evaluateStringLiteral(item);
            positionList.add(position);
            bufferedReordering.addExpected(position);
        }
    }

}
