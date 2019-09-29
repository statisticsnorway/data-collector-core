package no.ssb.dc.core.handler;

public class EndOfStreamException extends RuntimeException {
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
