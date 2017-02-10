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
      existingPayload = getPayload(existing, aggregator.getExistingType());
      incrementalPayload = getPayload(incremental, aggregator.getIncrementType());
      Map<String, Object> coreHeaders = getCoreHeaders(existingPayload, incrementalPayload);
      Payload aggregate = aggregator.aggregate(existingPayload, incrementalPayload);
      aggregate.getHeaders().putAll(coreHeaders);
      return createPayloadByteArray(aggregate, aggregator.getExistingType());
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

  /**
   * Returns payload transformation of the given byte[].
   * @param bytes
   * @param bodyType
   * @return null if byte[] or bodyType is missing or body inside payload is null.
   * @throws IOException
   * @throws ClassNotFoundException
     */
  public Payload getPayload(byte[] bytes, Class bodyType)
      throws IOException, ClassNotFoundException {
    if (bytes == null || bodyType == null) {
      return null;
    }
    Payload payload = typeConverterRegistry.lookup(Payload.class, byte[].class).convertTo(Payload.class, bytes);
    if (bodyType.equals(byte[].class)) {
      return payload;
    }
    if (payload.getBody() == null) {
      return null;
    }
    TypeConverter lookup = typeConverterRegistry.lookup(bodyType, byte[].class);
    if (lookup == null) {
      log.error("Type converter from byte[] to {} not found", bodyType);
      throw new RuntimeException("Type converter from byte[] to " + bodyType + " not found");
    }
    payload.setBody(lookup.convertTo(bodyType, payload.getBody()));
    return payload;
  }

  /**
   * Converts payload body into byte[] and returns byte[] transformation of the computed payload.
   * @param payload to be converted (can have a user difened class in body)
   * @param bodyType class type of the body inside payload.
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
     */
  public byte[] createPayloadByteArray(Payload payload, Class bodyType)
      throws IOException, ClassNotFoundException {
    TypeConverter lookup = typeConverterRegistry.lookup(byte[].class, bodyType);
    if (lookup != null) {
      byte[] payloadByte = lookup.convertTo(byte[].class, payload.getBody());
      Payload<byte[]> finalPayload = new Payload<>(payloadByte, payload.getHeaders());
      return typeConverterRegistry.lookup(byte[].class, Payload.class).convertTo(byte[].class, finalPayload);
    } else {
      log.error("Type converter from {} to byte[] not found.", bodyType);
      throw new RuntimeException("Type converter from " + bodyType + " to byte[] not found");
    }
  }
}
