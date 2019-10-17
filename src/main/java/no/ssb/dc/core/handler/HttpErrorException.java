package no.ssb.dc.core.handler;

import no.ssb.dc.api.error.ExecutionException;

public class HttpErrorException extends ExecutionException {
    public HttpErrorException() {
    }

    public HttpErrorException(String message) {
        super(message);
    }

    public HttpErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpErrorException(Throwable cause) {
        super(cause);
    }

    public HttpErrorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
