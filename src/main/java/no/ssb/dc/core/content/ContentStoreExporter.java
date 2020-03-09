package no.ssb.dc.core.content;

import io.prometheus.client.Counter;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import java.util.concurrent.Callable;

public class ContentStoreExporter {

    static final Counter writePaginationPageContent = Counter.build("content_write_pagination_page", "Content write pagination page")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    static final Counter bufferPaginationPageEntryContent = Counter.build("content_buffer_pagination_entry", "Content buffer pagination entry")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    static final Counter bufferPageEntryDocumentContent = Counter.build("content_buffer_entry_document", "Content buffer entry document")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    static final Counter publishPositionContent = Counter.build("content_publish_position", "Content publish position")
            .namespace("dc")
            .subsystem("content_stream")
            .labelNames("topic")
            .register();

    public static class Paginate {
        public static void intercept(@SuperCall Callable<?> zuper, @AllArguments Object[] args) throws Exception {
            String topic = (String) args[0];
            writePaginationPageContent.labels(topic).inc();
            zuper.call();
        }
    }

    public static class Entry {
        public static void intercept(@SuperCall Callable<?> zuper, @AllArguments Object[] args) throws Exception {
            String topic = (String) args[0];
            bufferPaginationPageEntryContent.labels(topic).inc();
            zuper.call();
        }
    }

    public static class Document {
        public static void intercept(@SuperCall Callable<?> zuper, @AllArguments Object[] args) throws Exception {
            String topic = (String) args[0];
            bufferPageEntryDocumentContent.labels(topic).inc();
            zuper.call();
        }
    }

    public static class Publish {
        public static void intercept(@SuperCall Callable<?> zuper, @AllArguments Object[] args) throws Exception {
            String topic = (String) args[0];
            String[] positions = (String[]) args[1];
            publishPositionContent.labels(topic).inc(positions.length);
            zuper.call();
        }
    }
}
