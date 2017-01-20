package org.apache.camel.cms.orchestrator.factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kartik.bommepally on 10/01/17.
 */
public class AsyncCallbackFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncCallbackFactory.class);
    private static String CALLBACK_ENDPOINT = null;

    public static void registerCallbackEndpoint(String callbackEndpoint) {
        if (CALLBACK_ENDPOINT != null) {
            LOG.warn("Callback endpoint is already registered " + CALLBACK_ENDPOINT);
            return;
        }
        CALLBACK_ENDPOINT = callbackEndpoint;
    }

    public static String getCallbackEndpoint() {
        return CALLBACK_ENDPOINT;
    }
}
