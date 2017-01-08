package org.apache.camel.cms.orchestrator.factory;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.cms.orchestrator.exception.AggregateStoreInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class AggregateStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AggregateStoreFactory.class);
    private static AggregateStore STORE_INSTANCE = null;

    public static void registerStore(AggregateStore aggregateStore)
    {
        if(STORE_INSTANCE!=null)
        {
            LOG.error("Aggregate Store is already registered");
            throw new AggregateStoreInitializationException("Aggregate Store is already registered");
        }
        STORE_INSTANCE = aggregateStore;
    }

    public static AggregateStore getStoreInstance() {
        return STORE_INSTANCE;
    }
}
