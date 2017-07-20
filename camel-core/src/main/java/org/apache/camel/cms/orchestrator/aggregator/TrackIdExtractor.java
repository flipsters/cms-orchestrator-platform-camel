package org.apache.camel.cms.orchestrator.aggregator;

import org.apache.camel.Exchange;

/**
 * Created by kartik.bommepally on 18/01/17.
 */
public interface TrackIdExtractor {
    String getTrackId(Exchange exchange);
}
