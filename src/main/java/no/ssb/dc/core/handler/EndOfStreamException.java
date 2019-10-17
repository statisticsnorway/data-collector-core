package no.ssb.dc.core.handler;

import no.ssb.dc.api.error.ExecutionException;

public class EndOfStreamException extends ExecutionException {

    public EndOfStreamException() {
        super();
    }

    public EndOfStreamException(String message) {
        super(message);
    }

    public EndOfStreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public EndOfStreamException(Throwable cause) {
        super(cause);
    }

    protected EndOfStreamException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
