package org.apache.camel.cms.orchestrator.exception;

/**
 * Created by pawas.kumar on 17/03/17.
 */
public class MappingStoreInitializationException extends RuntimeException {

    public MappingStoreInitializationException() {
    }

    public MappingStoreInitializationException(String message) {
        super(message);
    }

    public MappingStoreInitializationException(String message, Throwable cause) {
        super(message, cause);
    }

    public MappingStoreInitializationException(Throwable cause) {
        super(cause);
    }
}
