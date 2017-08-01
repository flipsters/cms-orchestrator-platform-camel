package org.apache.camel.cms.orchestrator.aggregator;

import java.io.IOException;

/**
 * Created by pawas.kumar on 10/07/17.
 */
public interface HeterogeneousPayloadAggregator<A,B,C> {

    /**
     * Aggregates the overall computed payload with the incremental.
     * @param existing computed payload so far, can be null.
     * @param increment incremental payload, can be null.
     * @return payload the user wants to increment
     * @throws IOException
     * @throws ClassNotFoundException
     */
    Payload<C> aggregate(Payload<A> existing, Payload<B> increment) throws Exception;

    /**
     * provide the type converters for the mentioned type to and from byte[].
     * @return the type of the overall computed payload. This would be used for deserialization.
     */
    Class<A> getExistingType();

    /**
     * provide the type converters for the mentioned type to and from byte[].
     * @return the type of the incremental payload. This would be used for deserialization.
     */
    Class<B> getIncrementType();

    /**
     * provide the type converters for the mentioned type to and from byte[].
     * @return the type of the output payload. This would be used for deserialization.
     */
    Class<C> getOutputType();

    /**
     * return the aggregator ID which would be referenced in the aggregator store.
     * @return
     */
    String getId();
}
