package org.apache.camel.cms.orchestrator.factory;

import flipkart.cms.aggregator.client.AggregateStore;
import flipkart.cms.orchestrator.status.store.api.StatusStoreService;
import org.apache.camel.cms.orchestrator.exception.AggregateStoreInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class StatusStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(StatusStoreFactory.class);
    private static StatusStoreService STORE_INSTANCE = null;

    public static void registerStore(StatusStoreService statusStoreService)
    {
        if(STORE_INSTANCE!=null)
        {
            LOG.error("Status Store is already registered");
            throw new AggregateStoreInitializationException("Status Store is already registered");
        }
        STORE_INSTANCE = statusStoreService;
    }

    public static StatusStoreService getStoreInstance() {
        return STORE_INSTANCE;
    }
}
