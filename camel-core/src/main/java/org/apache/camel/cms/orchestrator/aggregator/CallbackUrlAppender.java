package org.apache.camel.cms.orchestrator.aggregator;

import org.apache.camel.Exchange;

/**
 * Created by pawas.kumar on 02/03/17.
 */
public interface CallbackUrlAppender {
    String mergeCallback(Exchange exchange) throws Exception;
}
