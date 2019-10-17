package no.ssb.dc.core.handler;

import no.ssb.dc.api.error.ExecutionException;

public class ParallelException extends ExecutionException {
    public ParallelException() {
        super();
    }

    public ParallelException(String message) {
        super(message);
    }

    public ParallelException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParallelException(Throwable cause) {
        super(cause);
    }

    protected ParallelException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
