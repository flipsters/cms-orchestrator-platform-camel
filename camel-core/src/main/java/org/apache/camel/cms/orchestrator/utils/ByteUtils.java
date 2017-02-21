package org.apache.camel.cms.orchestrator.utils;

import com.google.common.collect.Maps;
import org.apache.camel.Exchange;
import org.apache.camel.TypeConverter;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.spi.TypeConverterRegistry;

/**
 * Created by pawas.kumar on 17/01/17.
 */
public class ByteUtils {

  public static Payload createPayload(Exchange exchange) {
    return new Payload(exchange.getIn().getBody(byte[].class), exchange.getIn().getHeaders());
  }

  public static byte[] getByteArrayFromPayload(TypeConverterRegistry typeConverterRegistry, Payload payload) {
    TypeConverter lookup = typeConverterRegistry.lookup(byte[].class, Payload.class);
    if (lookup == null) {
      throw new RuntimeException("Type converter from " + payload.getClass() + " to byte[] not found");
    }
    return lookup.convertTo(byte[].class, payload);
  }

  public static Payload createPayloadFromBody(TypeConverterRegistry typeConverterRegistry, Object body) {
    Class bodyType = body.getClass();
    byte[] bytes = typeConverterRegistry.lookup(byte[].class, bodyType).convertTo(byte[].class, body);
    return new Payload(bytes, Maps.newHashMap());
  }

  public static Payload createPayloadFromByteArray(TypeConverterRegistry typeConverterRegistry, byte[] payload) {
    TypeConverter lookup = typeConverterRegistry.lookup(Payload.class, byte[].class);
    return lookup.convertTo(Payload.class, payload);
  }
}
