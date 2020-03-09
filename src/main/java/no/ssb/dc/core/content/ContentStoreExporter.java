package no.ssb.dc.core.content;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import java.util.concurrent.Callable;

/**
 * Metrics:
 * <p>
 * pagination_page_publish_count
 * pagination_page_public_duration
 * buffer_pagination_entry_count
 * buffer_entry_document_count
 * pagination_entry_publish_position_count
 * pagination_entry_publish_position_duration
 */
public class ContentStoreExporter {

    static final Counter writePaginationPageContent = Counter.build("pagination_page_publish_count", "Number of written pagination pages")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    static final Histogram writePaginationPageContentDuration = Histogram.build("pagination_page_publish_duration", "Pagination page write duration")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    static final Counter bufferPaginationPageEntryContent = Counter.build("buffer_pagination_entry_count", "Buffer pagination entry count")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    static final Counter bufferPageEntryDocumentContent = Counter.build("buffer_entry_document_count", "Buffer entry document count")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    static final Counter publishPositionContent = Counter.build("pagination_entry_publish_position_count", "Number of published positions")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    static final Histogram publishPositionContentDuration = Histogram.build("pagination_entry_publish_position_duration", "Published position write duration")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    public static class Paginate {
        public static void intercept(@SuperCall Callable<?> zuper, @AllArguments Object[] args) throws Exception {
            String topic = (String) args[0];
            Histogram.Timer timer = writePaginationPageContentDuration.labels(topic).startTimer();
            try {
                zuper.call();
            } finally {
                timer.observeDuration();
                writePaginationPageContent.labels(topic).inc();
            }
        }
    }

    public static class Entry {
        public static void intercept(@SuperCall Callable<?> zuper, @AllArguments Object[] args) throws Exception {
            String topic = (String) args[0];
            zuper.call();
            bufferPaginationPageEntryContent.labels(topic).inc();
        }
    }

    public static class Document {
        public static void intercept(@SuperCall Callable<?> zuper, @AllArguments Object[] args) throws Exception {
            String topic = (String) args[0];
            zuper.call();
            bufferPageEntryDocumentContent.labels(topic).inc();
        }
    }

    public static class Publish {
        public static void intercept(@SuperCall Callable<?> zuper, @AllArguments Object[] args) throws Exception {
            String topic = (String) args[0];
            String[] positions = (String[]) args[1];
            Histogram.Timer timer = publishPositionContentDuration.labels(topic).startTimer();
            try {
                zuper.call();
            } finally {
                timer.observeDuration();
                publishPositionContent.labels(topic).inc(positions.length);
            }

        }
    }
}
