package no.ssb.dc.core.handler;

import no.ssb.dc.api.ConfigurationMap;
import no.ssb.dc.api.Position;
import no.ssb.dc.api.PositionProducer;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.Publish;
import no.ssb.dc.core.executor.BufferedReordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

@Handler(forClass = Publish.class)
public class PublishHandler extends AbstractHandler<Publish> {

    private static final Logger LOG = LoggerFactory.getLogger(PublishHandler.class);

    public PublishHandler(Publish node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        ExpressionLanguage el = new ExpressionLanguage(input.variables());

        String positionVariable = (String) el.evaluateExpression(node.positionVariableExpression());

        BufferedReordering<Position<?>> bufferedReordering = input.services().get(BufferedReordering.class);

        ConfigurationMap config = input.services().get(ConfigurationMap.class);
        ContentStore contentStore = input.services().get(ContentStore.class);
        String namespace = config.get("namespace.default");
        Set<String> contentKeys = contentStore.contentKeys(namespace, positionVariable);

        PositionProducer<?> positionProducer = input.state(PositionProducer.class);

        if (!contentKeys.isEmpty()) {
            bufferedReordering.addCompleted(positionProducer.produce(positionVariable), orderedPositions -> {
                contentStore.publish(namespace, orderedPositions.stream().map(Position::asString).collect(Collectors.joining()));
                LOG.info("Published: [{}] with content [{}]",
                        orderedPositions.stream().map(Position::asString).collect(Collectors.joining(",")),
                        contentKeys.stream().collect(Collectors.joining(","))
                );
            });
        }

        return ExecutionContext.empty();
    }

}
