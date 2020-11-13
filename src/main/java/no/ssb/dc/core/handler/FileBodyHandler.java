package no.ssb.dc.core.handler;

import no.ssb.dc.api.http.BodyHandler;
import no.ssb.dc.api.util.CompressUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Flow;

public class FileBodyHandler implements BodyHandler<Path> {

    private static final Logger LOG = LoggerFactory.getLogger(FileBodyHandler.class);

    private final Object lock = new Object();
    protected final Path file;
    private volatile Flow.Subscription subscription;

    public FileBodyHandler(Path file) {
        Objects.requireNonNull(file);
        this.file = file;
        if (!Files.isWritable(file)) {
            throw new IllegalStateException("The file '" + file.toString() + "' is NOT readable!");
        }
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (this.subscription != null) {
            subscription.cancel();
            return;
        }
        this.subscription = subscription;
        // We can handle whatever you've got
        subscription.request(Long.MAX_VALUE);
        LOG.info("Start download: {}", file);
    }

    public boolean hasRemaining(List<ByteBuffer> byteBuffers) {
        synchronized (lock) {
            for (ByteBuffer buf : byteBuffers) {
                if (buf.hasRemaining())
                    return true;
            }
        }
        return false;
    }

    private void appendToFile(List<ByteBuffer> byteBuffers) {
        synchronized (lock) {
            try {
                for (ByteBuffer buf : byteBuffers) {
                    byte[] bytes = new byte[buf.remaining()];
                    buf.get(bytes);
                    Files.write(file, bytes, StandardOpenOption.APPEND);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void onNext(List<ByteBuffer> byteBuffers) {
        // incoming buffers are allocated by http client internally,
        // and won't be used anywhere except this place.
        // So it's free simply to store them for further processing.
        assert hasRemaining(byteBuffers);
        appendToFile(byteBuffers);
    }

    @Override
    public void onError(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        throw new RuntimeException(throwable);
    }

    @Override
    public void onComplete() {
        // try decompress received payload
        LOG.info("Download completed: {}", file);
        synchronized (lock) {
            tryDecompress();
        }
    }

    private void tryDecompress() {
        if (!CompressUtils.isGzipCompressed(file)) {
            return;
        }

        try {
            LOG.info("Decompress file: {}", file);
            Path tempFile = Files.createTempFile(UUID.randomUUID().toString(), null);
            if (!Files.isWritable(tempFile)) {
                throw new RuntimeException("Decompression temp file is not writable: " + tempFile.toString());
            }

            CompressUtils.gunzip(file, new FileOutputStream(tempFile.toFile()));

            LOG.info("Move file from: {} to {}", tempFile, file);
            Files.move(tempFile, file, StandardCopyOption.ATOMIC_MOVE);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static FileBodyHandler ofFile(Path file) {
        return new FileBodyHandler(file);
    }

    public Path getFile() {
        return file;
    }

    @Override
    public Path body() {
        return getFile();
    }
}
