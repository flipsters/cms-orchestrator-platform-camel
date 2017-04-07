package org.apache.camel.cms.orchestrator.exception;

/**
 * Created by pawas.kumar on 06/04/17.
 */
public class ExpiryStoreInitializationException extends RuntimeException {
    public ExpiryStoreInitializationException() {
    }

    public ExpiryStoreInitializationException(String message) {
        super(message);
    }

    public ExpiryStoreInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExpiryStoreInitializationException(Throwable cause) {
        super(cause);
    }
}
