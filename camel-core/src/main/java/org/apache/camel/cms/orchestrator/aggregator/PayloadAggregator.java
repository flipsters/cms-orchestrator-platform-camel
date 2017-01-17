package org.apache.camel.cms.orchestrator.aggregator;

import java.io.IOException;

/**
 * Created by pawas.kumar on 16/01/17.
 */
public interface PayloadAggregator {

  Payload aggregate(Payload existing, Payload increment) throws IOException, ClassNotFoundException;

  String getId();

}
