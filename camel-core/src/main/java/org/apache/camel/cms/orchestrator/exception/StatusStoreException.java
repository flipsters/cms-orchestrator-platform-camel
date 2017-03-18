package org.apache.camel.cms.orchestrator.exception;

/**
 * TODO : check if it is being used
 * Created by achit.ojha on 08/01/17.
 */
public class StatusStoreException extends Exception {
    public StatusStoreException() {
    }

    public StatusStoreException(String message) {
        super(message);
    }

    public StatusStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public StatusStoreException(Throwable cause) {
        super(cause);
    }
}
