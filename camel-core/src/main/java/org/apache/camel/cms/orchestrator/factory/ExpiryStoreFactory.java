package org.apache.camel.cms.orchestrator.factory;

import flipkart.cms.aggregator.client.ExpiryStore;
import flipkart.cms.aggregator.client.NoOpExpiryStoreImpl;
import org.apache.camel.cms.orchestrator.exception.ExpiryStoreInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pawas.kumar on 06/04/17.
 */
public class ExpiryStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ExpiryStoreFactory.class);
    private static ExpiryStore STORE_INSTANCE = null;
    private static ExpiryStore NO_OP_STORE_INSTANCE = new NoOpExpiryStoreImpl();

    public static void register(ExpiryStore expiryStore)
    {
        if(STORE_INSTANCE!=null)
        {
            LOG.error("Expiry Store is already registered");
            throw new ExpiryStoreInitializationException("Expiry Store is already registered");
        }
        STORE_INSTANCE = expiryStore;
    }

    public static ExpiryStore getStoreInstance(Long expiryBreachTime) {
        if (expiryBreachTime == null) {
            return NO_OP_STORE_INSTANCE;
        } else {
            return STORE_INSTANCE;
        }
    }
}
