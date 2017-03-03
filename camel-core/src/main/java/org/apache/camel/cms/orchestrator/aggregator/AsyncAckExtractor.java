package org.apache.camel.cms.orchestrator.aggregator;

import org.apache.camel.Exchange;

/**
 * Created by pawas.kumar on 01/03/17.
 */
public interface AsyncAckExtractor {
    RequestIdentifier getRequestIdentifier(Exchange exchange);
}
