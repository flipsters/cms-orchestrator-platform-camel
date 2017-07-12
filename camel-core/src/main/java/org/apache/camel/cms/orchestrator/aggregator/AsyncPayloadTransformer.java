package org.apache.camel.cms.orchestrator.aggregator;

import org.apache.camel.Exchange;

/**
 * Created by pawas.kumar on 02/03/17.
 */
public interface AsyncPayloadTransformer {
    String transform(Exchange exchange) throws Exception;
}
