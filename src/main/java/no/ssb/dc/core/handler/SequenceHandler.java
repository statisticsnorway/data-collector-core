package no.ssb.dc.core.handler;

import no.ssb.dc.api.PageContext;
import no.ssb.dc.api.PositionObserver;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.DocumentParserFeature;
import no.ssb.dc.api.handler.Handler;
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
            doTokenizerQuery(input, response, positionList);

        } else {
            // split by query
            doSplitQuery(input, response, positionList);
        }

        if (positionList.isEmpty()) {
            LOG.warn("Reached end of stream! No more elements from position: {}", pageContextBuilder.nextPositionVariableNames().stream().map(name -> name + "=" + input.variable(name)).collect(Collectors.toList()));
            throw new EndOfStreamException();
        }

        for (String position : positionList) {
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

    void doTokenizerQuery(ExecutionContext input, Response response, List<String> positionList) {
        try {
            TempFileBodyHandler fileBodyHandler = (TempFileBodyHandler) response.<Path>bodyHandler().orElseThrow();
            DocumentParserFeature parser = Queries.parserFor(node.splitToListQuery().getClass());

            try (FileInputStream fis = new FileInputStream(fileBodyHandler.body().toFile())) {
                parser.tokenDeserializer(fis, entry -> {
                    String position = Queries.from(input, node.expectedQuery()).evaluateStringLiteral(entry);
                    positionList.add(position);
                });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void doSplitQuery(ExecutionContext input, Response response, List<String> positionList) {
        List<?> splitToListItemList = Queries.from(input, node.splitToListQuery()).evaluateList(response.body());

        for (Object item : splitToListItemList) {
            String position = Queries.from(input, node.expectedQuery()).evaluateStringLiteral(item);
            positionList.add(position);
        }
    }

}
