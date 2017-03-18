package org.apache.camel.cms.orchestrator.exception;

/**
 * Created by pawas.kumar on 17/03/17.
 */
public class StatusStoreInitializationException extends RuntimeException {

    public StatusStoreInitializationException() {
    }

    public StatusStoreInitializationException(String message) {
        super(message);
    }

    public StatusStoreInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public StatusStoreInitializationException(Throwable cause) {
        super(cause);
    }
}
