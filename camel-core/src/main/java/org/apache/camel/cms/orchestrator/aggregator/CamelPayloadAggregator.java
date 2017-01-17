package org.apache.camel.cms.orchestrator.aggregator;

import flipkart.cms.aggregator.client.Aggregator;
import lombok.AllArgsConstructor;

import java.io.*;
import java.util.Arrays;

/**
 * Created by pawas.kumar on 16/01/17.
 */
@AllArgsConstructor
public class CamelPayloadAggregator implements Aggregator {

  PayloadAggregator aggregator;

  @Override
  public byte[] aggregate(byte[] existing, byte[] incremental) {
    Payload existingPayload, incrementalPayload;
    try {
      existingPayload = getPayload(existing);
      incrementalPayload = getPayload(incremental);
      Payload aggregate = aggregator.aggregate(existingPayload, incrementalPayload);
      return aggregate.getBytes();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Not able to deserialize bytes : " + Arrays.toString(existing) + " and " + Arrays.toString(incremental));
    }
  }

  @Override
  public String getId() {
    return aggregator.getId();
  }

  private Payload getPayload(byte[] bytes)
      throws IOException, ClassNotFoundException {
    Payload obj = null;
    ByteArrayInputStream bis = null;
    ObjectInputStream ois = null;
    try {
      bis = new ByteArrayInputStream(bytes);
      ois = new ObjectInputStream(bis);
      obj =
          (Payload) ois.readObject();
    } finally {
      if (bis != null) {
        bis.close();
      }
      if (ois != null) {
        ois.close();
      }
    }
    return obj;
  }


}
