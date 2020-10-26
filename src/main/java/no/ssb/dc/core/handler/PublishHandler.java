package no.ssb.dc.core.handler;

import no.ssb.dc.api.PositionObserver;
import no.ssb.dc.api.content.ContentStore;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.el.ExpressionLanguage;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.node.Publish;
import no.ssb.dc.core.executor.BufferedReordering;
import no.ssb.dc.core.health.HealthWorkerMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

@Handler(forClass = Publish.class)
public class PublishHandler extends AbstractNodeHandler<Publish> {

    private static final Logger LOG = LoggerFactory.getLogger(PublishHandler.class);

    public PublishHandler(Publish node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext input) {
        super.execute(input);
        ExpressionLanguage el = new ExpressionLanguage(input);

        String positionValue = (String) el.evaluateExpression(node.positionVariableExpression());

        BufferedReordering<String> bufferedReordering = input.services().get(BufferedReordering.class);
        //LOG.trace("Expected-Sequence: {}", bufferedReordering.expected());

        ContentStore contentStore = input.services().get(ContentStore.class);
        String topicName = node.configurations().flowContext().topic();
        if (topicName == null) {
            throw new IllegalStateException("Unable to resolve topic!");
        }
        Set<String> contentKeys = contentStore.contentKeys(topicName, positionValue);
        //LOG.trace("Content-Keys: {}", contentKeys);

        HealthWorkerMonitor monitor = input.services().get(HealthWorkerMonitor.class);

        if (!contentKeys.isEmpty()) {
            contentStore.lock(topicName);
            try {
                //LOG.trace("Add-Completed: {}", positionValue);
                bufferedReordering.addCompleted(positionValue, orderedPositions -> {
                    if (monitor != null && monitor.contentStream().hasNotSetStartPosition()) {
                        monitor.contentStream().setStartPosition(orderedPositions.get(0));
                    }

                    contentStore.publish(topicName, orderedPositions.toArray(new String[0]));

                    if (LOG.isInfoEnabled()) {
                        LOG.info("Reordered sequence: [{}] with content [{}]",
                                String.join(",", orderedPositions),
                                String.join(",", contentKeys)
                        );
                    }
                    PositionObserver positionObserver = input.state(PositionObserver.class);
                    // only avail in scope if coming from paginate
                    if (positionObserver != null) {
                        positionObserver.completed(orderedPositions.size());
                    }

                    if (monitor != null) {
                        monitor.contentStream().setLastPosition(orderedPositions.get(orderedPositions.size() - 1));
                    }
                });
            } finally {
                contentStore.unlock(topicName);
            }
        }

        return ExecutionContext.empty();
    }

}
