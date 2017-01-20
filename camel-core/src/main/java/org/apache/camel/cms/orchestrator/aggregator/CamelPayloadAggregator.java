package org.apache.camel.cms.orchestrator.aggregator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import flipkart.cms.aggregator.client.Aggregator;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.utils.ByteUtils;
import org.apache.camel.cms.orchestrator.utils.OrchestratorUtils;
import org.apache.camel.converter.ObjectConverter;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by pawas.kumar on 16/01/17.
 */
@AllArgsConstructor
@Slf4j
public class CamelPayloadAggregator implements Aggregator {

  private PayloadAggregator aggregator;

  @Override
  public byte[] aggregate(byte[] existing, byte[] incremental) {
    Payload existingPayload, incrementalPayload;
    try {
      existingPayload = getPayload(existing);
      incrementalPayload = getPayload(incremental);
      Map<String, Object> coreHeaders = getCoreHeaders(existingPayload, incrementalPayload);
      Payload aggregate = aggregator.aggregate(existingPayload, incrementalPayload);
      aggregate.getHeaders().putAll(coreHeaders);
      return ByteUtils.getBytes(aggregate);
    } catch (IOException | ClassNotFoundException e) {
      log.error("Not able to deserialize the byte array!!");
      throw new RuntimeException("Not able to deserialize bytes");
    }
  }

  private Map<String, Object> getCoreHeaders(Payload existing, Payload increment) {
    Map<String, Object> stringObjectMap = addHeaders(existing, Maps.<String, Object>newHashMap());
    return addHeaders(increment, stringObjectMap);
  }

  private Map<String, Object> addHeaders(Payload payload, Map<String, Object> headerMap) {
    if (payload != null) {
      Map<String, Object> headers = payload.getHeaders();
      for (String title : OrchestratorUtils.getHeaderTitles()) {
        headerMap.put(title, headers.get(title));
      }
    }
    return headerMap;
  }

  @Override
  public String getId() {
    return aggregator.getId();
  }

  public static Payload getPayload(byte[] bytes)
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
