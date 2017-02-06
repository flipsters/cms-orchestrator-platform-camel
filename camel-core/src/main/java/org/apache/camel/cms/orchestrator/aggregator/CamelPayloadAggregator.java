package org.apache.camel.cms.orchestrator.aggregator;

import com.google.common.collect.Maps;
import flipkart.cms.aggregator.client.Aggregator;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.TypeConverter;
import org.apache.camel.cms.orchestrator.utils.OrchestratorUtils;
import org.apache.camel.spi.TypeConverterRegistry;

import java.io.*;
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
      existingPayload = getPayload(existing);
      incrementalPayload = getPayload(incremental);
      Map<String, Object> coreHeaders = getCoreHeaders(existingPayload, incrementalPayload);
      Payload aggregate = aggregator.aggregate(existingPayload, incrementalPayload);
      aggregate.getHeaders().putAll(coreHeaders);
      return createPayloadByteArray(aggregate);
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
      for (String title : OrchestratorUtils.getCoreHeaderTitles()) {
        if (headers.get(title) != null) {
          headerMap.put(title, headers.get(title));
        }
      }
    }
    return headerMap;
  }

  @Override
  public String getId() {
    return aggregator.getId();
  }

  public Payload getPayload(byte[] bytes)
      throws IOException, ClassNotFoundException {
    Payload payload = typeConverterRegistry.lookup(Payload.class, byte[].class).convertTo(Payload.class, bytes);
    Class bodyType = payload.getBodyType();
    if (bodyType == null) {
      return null;
    }
    TypeConverter lookup = typeConverterRegistry.lookup(bodyType, byte[].class);
    if (lookup == null) {
      log.error("Type converter from byte[] to {} not found", payload.getBodyType());
      throw new RuntimeException("Type converter from byte[] to " + payload.getBodyType() + "not found");
    }
    payload.setBody(lookup.convertTo(bodyType, payload.getBody()));
    return payload;
  }

  public byte[] createPayloadByteArray(Payload payload)
      throws IOException, ClassNotFoundException {
    TypeConverter lookup = typeConverterRegistry.lookup(byte[].class, payload.getBodyType());
    if (lookup != null) {
      byte[] payloadByte = lookup.convertTo(byte[].class, payload.getBody());
      Payload<byte[]> finalPayload = new Payload<>(payloadByte, payload.getHeaders(), payload.getBodyType());
      return typeConverterRegistry.lookup(byte[].class, Payload.class).convertTo(byte[].class, finalPayload);
    } else {
      log.error("Type converter from {} to byte[] not found", payload.getBodyType());
      throw new RuntimeException("Type converter from " + payload.getBodyType() + " to byte[] not found");
    }
  }
}
