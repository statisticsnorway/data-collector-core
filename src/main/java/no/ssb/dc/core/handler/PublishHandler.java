package no.ssb.dc.core.handler;

import no.ssb.dc.api.Position;
import no.ssb.dc.api.PositionObserver;
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
public class PublishHandler extends AbstractNodeHandler<Publish> {

    private static final Logger LOG = LoggerFactory.getLogger(PublishHandler.class);

    public PublishHandler(Publish node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        ExpressionLanguage el = new ExpressionLanguage(input.variables());

        String positionVariable = (String) el.evaluateExpression(node.positionVariableExpression());

        BufferedReordering<Position<?>> bufferedReordering = input.services().get(BufferedReordering.class);

        ContentStore contentStore = input.services().get(ContentStore.class);
        String topicName = node.configurations().flowContext().topic();
        if (topicName == null) {
            throw new IllegalStateException("Unable to resolve topic!");
        }
        Set<String> contentKeys = contentStore.contentKeys(topicName, positionVariable);

        PositionProducer<?> positionProducer = input.state(PositionProducer.class);

        if (!contentKeys.isEmpty()) {
            bufferedReordering.addCompleted(positionProducer.produce(positionVariable), orderedPositions -> {
                contentStore.publish(topicName, orderedPositions.stream().map(Position::asString).toArray(String[]::new));
                if (LOG.isInfoEnabled()) {
                    LOG.info("Reordered sequence: [{}] with content [{}]",
                            orderedPositions.stream().map(Position::asString).collect(Collectors.joining(",")),
                            String.join(",", contentKeys)
                    );
                }
                PositionObserver positionObserver = input.state(PositionObserver.class);
                positionObserver.completed(orderedPositions.size());
            });
        }

        return ExecutionContext.empty();
    }

}
