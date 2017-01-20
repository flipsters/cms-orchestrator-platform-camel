package org.apache.camel.cms.orchestrator.aggregator;

import java.io.IOException;

/**
 * Created by pawas.kumar on 16/01/17.
 */
public interface PayloadAggregator {

  /**
   * @param existing can be null
   * @param increment incremental payload
   * @return payload the user wants to increment
   * @throws IOException
   * @throws ClassNotFoundException
   */
  Payload aggregate(Payload existing, Payload increment) throws IOException, ClassNotFoundException;

  String getId();

}
