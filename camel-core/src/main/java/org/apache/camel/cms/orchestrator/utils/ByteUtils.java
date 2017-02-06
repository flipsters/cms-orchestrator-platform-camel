package org.apache.camel.cms.orchestrator.utils;

import org.apache.camel.Exchange;
import org.apache.camel.TypeConverter;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.spi.TypeConverterRegistry;

import java.io.*;

/**
 * Created by pawas.kumar on 17/01/17.
 */
public class ByteUtils {

  public static Payload createPayload(Exchange exchange) {
    Class bodyType = null;
    if (exchange.getIn().getBody() != null) {
      bodyType = exchange.getIn().getBody().getClass();
    }
    return new Payload(exchange.getIn().getBody(byte[].class), exchange.getIn().getHeaders(), bodyType);
  }

  public static byte[] getByteArrayFromPayload(TypeConverterRegistry typeConverterRegistry, Payload payload) {
    TypeConverter lookup = typeConverterRegistry.lookup(byte[].class, Payload.class);
    if (lookup == null) {
      throw new RuntimeException("Type converter from " + payload.getBodyType() + " to byte[] not found");
    }
    return lookup.convertTo(byte[].class, payload);
  }
}
