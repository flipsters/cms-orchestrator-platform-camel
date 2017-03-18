package org.apache.camel.cms.orchestrator.factory;

import flipkart.cms.aggregator.client.MappingStore;
import org.apache.camel.cms.orchestrator.exception.MappingStoreInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pawas.kumar on 17/03/17.
 */
public class MappingStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MappingStoreFactory.class);
    private static MappingStore STORE_INSTANCE = null;

    public static void registerStore(MappingStore mappingStore)
    {
        if(STORE_INSTANCE!=null)
        {
            LOG.error("Mapping Store is already registered");
            throw new MappingStoreInitializationException("Mapping Store is already registered");
        }
        STORE_INSTANCE = mappingStore;
    }

    public static MappingStore getStoreInstance() {
        return STORE_INSTANCE;
    }
}
