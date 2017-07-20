package org.apache.camel.cms.orchestrator.aggregator;

import java.io.IOException;

/**
 * Created by pawas.kumar on 16/01/17.
 */
public interface PayloadAggregator<I,O> {

  /**
   * Aggregates the overall computed payload with the incremental.
   * @param existing computed payload so far, can be null.
   * @param increment incremental payload, can be null.
   * @return payload the user wants to increment
   * @throws IOException
   * @throws ClassNotFoundException
   */
  Payload<O> aggregate(Payload<O> existing, Payload<I> increment) throws IOException, ClassNotFoundException;

  /**
   * provide the type converters for the mentioned type to and from byte[].
   * @return the type of the overall computed payload. This would be used for deserialization.
     */
  Class<O> getExistingType();

  /**
   * provide the type converters for the mentioned type to and from byte[].
   * @return the type of the incremental payload. This would be used for deserialization.
     */
  Class<I> getIncrementType();

  /**
   * return the aggregator ID which would be referenced in the aggregator store.
   * @return
     */
  String getId();

}
