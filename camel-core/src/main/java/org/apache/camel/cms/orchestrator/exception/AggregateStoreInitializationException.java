package org.apache.camel.cms.orchestrator.exception;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class AggregateStoreInitializationException extends RuntimeException {
    public AggregateStoreInitializationException() {
    }

    public AggregateStoreInitializationException(String message) {
        super(message);
    }

    public AggregateStoreInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AggregateStoreInitializationException(Throwable cause) {
        super(cause);
    }
}
