package no.ssb.dc.core.executor;

import no.ssb.dc.api.error.ExecutionException;

public class WorkerException extends ExecutionException {

    public WorkerException(String message) {
        super(message);
    }

    public WorkerException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkerException(Throwable cause) {
        super(cause);
    }
}
