package no.ssb.dc.core.handler;

import com.github.mizosoft.methanol.MultipartBodyPublisher;
import no.ssb.dc.api.context.ExecutionContext;
import no.ssb.dc.api.handler.Handler;
import no.ssb.dc.api.http.Request;
import no.ssb.dc.api.node.BodyPart;
import no.ssb.dc.api.node.BodyPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Flow;

@Handler(forClass = BodyPublisher.class)
public class BodyPublisherHandler extends AbstractHandler<BodyPublisher> {

    private static final Logger LOG = LoggerFactory.getLogger(BodyPublisherHandler.class);

    public BodyPublisherHandler(BodyPublisher node) {
        super(node);
    }

    @Override
    public ExecutionContext execute(ExecutionContext context) {
        Request.Builder requestBuilder = context.state(Request.Builder.class);
        if (requestBuilder == null) {
            LOG.error("Unable to resolve Request.Builder!");
            return ExecutionContext.empty();
        }

        Flow.Publisher<ByteBuffer> byteArrayBodyPublisher;

        switch (node.getEncoding()) {
            case TEXT_PLAIN:
                byteArrayBodyPublisher = HttpRequest.BodyPublishers.ofByteArray(node.getPlainText().getBytes());
                break;

            case APPLICATION_X_WWW_FORM_URLENCODED:
                requestBuilder.header("Content-Type", node.getEncoding().getMimeType());
                byte[] bytes = Optional.ofNullable(node.getUrlEncodedData()).map(String::getBytes).orElse(new byte[0]);
                byteArrayBodyPublisher = HttpRequest.BodyPublishers.ofByteArray(bytes);
                break;

            case MULTIPART_FORM_DATA:
                MultipartBodyPublisher.Builder builder = MultipartBodyPublisher.newBuilder();
                for (BodyPart part : node.getParts()) {
                    if (part.isTextPart()) {
                        builder.textPart(part.name, part.value);

                    } else if (part.isFormPart()) {
                        Objects.requireNonNull(part.filename);

                        if (part.value instanceof byte[]) {
                            byte[] partBytes = (byte[]) part.value;
                            builder.formPart(part.name, part.filename, HttpRequest.BodyPublishers.ofByteArray(partBytes));

                        } else if (part.value instanceof String) {
                            String partString = (String) part.value;
                            builder.formPart(part.name, part.filename, HttpRequest.BodyPublishers.ofByteArray(partString.getBytes()));

                        } else {
                            throw new UnsupportedOperationException();
                        }
                    }
                }
                MultipartBodyPublisher multipartBodyPublisher = builder.build();

                requestBuilder.header("Content-Type", node.getEncoding().getMimeType() + ";boundary=" + multipartBodyPublisher.boundary());
                byteArrayBodyPublisher = multipartBodyPublisher;
                break;

            default:
                throw new UnsupportedOperationException();
        }

        return ExecutionContext.empty().state(BodyPublisher.BODY_PUBLISHER_RESULT, byteArrayBodyPublisher);
    }
}
