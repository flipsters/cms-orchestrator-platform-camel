package org.apache.camel.cms.orchestrator.exception;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class NoRequestIdPresentException extends Exception {

    public NoRequestIdPresentException() {
    }

    public NoRequestIdPresentException(String message) {
        super(message);
    }

    public NoRequestIdPresentException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoRequestIdPresentException(Throwable cause) {
        super(cause);
    }
}
