package org.apache.camel.cms.orchestrator.aggregator;

import flipkart.cms.aggregator.client.Aggregator;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.spi.TypeConverterRegistry;

import java.io.IOException;
import java.util.Map;

/**
 * Created by pawas.kumar on 16/01/17.
 */
@AllArgsConstructor
@Slf4j
public class CamelPayloadAggregator<I,O> implements Aggregator {

  private PayloadAggregator<I,O> aggregator;

  private TypeConverterRegistry typeConverterRegistry;

  @Override
  public byte[] aggregate(byte[] existing, byte[] incremental) {
    Payload<O> existingPayload;
    Payload<I> incrementalPayload;
    try {
      existingPayload = PayloadUtils.getPayload(existing, aggregator.getExistingType(), typeConverterRegistry);
      incrementalPayload = PayloadUtils.getPayload(incremental, aggregator.getIncrementType(), typeConverterRegistry);
      Map<String, Object> coreHeaders = PayloadUtils.getCoreHeaders(existingPayload, incrementalPayload);
      Payload aggregate = aggregator.aggregate(existingPayload, incrementalPayload);
      aggregate.getHeaders().putAll(coreHeaders);
      return PayloadUtils.createPayloadByteArray(aggregate, aggregator.getExistingType(), typeConverterRegistry);
    } catch (IOException | ClassNotFoundException e) {
      log.error("Not able to deserialize the byte array!!");
      throw new RuntimeException("Not able to deserialize bytes");
    }
  }

  @Override
  public String getId() {
    return aggregator.getId();
  }
}
