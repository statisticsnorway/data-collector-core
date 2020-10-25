package no.ssb.dc.core.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

class TempFileBodyHandler extends FileBodyHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TempFileBodyHandler.class);

    public TempFileBodyHandler() {
        super(createTempFile());
    }

    static Path createTempFile() {
        try {
            return Files.createTempFile(UUID.randomUUID().toString(), null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void dispose() {
        try {
            if (!Files.deleteIfExists(this.file)) {
                throw new RuntimeException("Unable to remove file: " + this.file);
            }
            LOG.trace("Remove temp file: {}", this.file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static TempFileBodyHandler ofFile() {
        return new TempFileBodyHandler();
    }
}
