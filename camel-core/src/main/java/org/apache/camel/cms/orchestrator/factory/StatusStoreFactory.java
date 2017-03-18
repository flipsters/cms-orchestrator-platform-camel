package org.apache.camel.cms.orchestrator.factory;

import flipkart.cms.aggregator.client.StatusStore;
import org.apache.camel.cms.orchestrator.exception.StatusStoreInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class StatusStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StatusStoreFactory.class);
    private static StatusStore STORE_INSTANCE = null;

    public static void registerStore(StatusStore statusStore)
    {
        if(STORE_INSTANCE!=null)
        {
            LOG.error("Status Store is already registered");
            throw new StatusStoreInitializationException("Status Store is already registered");
        }
        STORE_INSTANCE = statusStore;
    }

    public static StatusStore getStoreInstance() {
        return STORE_INSTANCE;
    }
}
