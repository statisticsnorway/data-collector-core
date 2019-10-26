package no.ssb.dc.core.handler;

import no.ssb.dc.api.error.ExecutionException;

public class HttpRequestTimeoutException extends ExecutionException {
    public HttpRequestTimeoutException(String message) {
        super(message);
    }
}
