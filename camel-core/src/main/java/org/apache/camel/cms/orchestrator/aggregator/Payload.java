package org.apache.camel.cms.orchestrator.aggregator;

import com.google.common.collect.Maps;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

/**
 * Created by pawas.kumar on 16/01/17.
 */
public class Payload implements Serializable {

  public Payload(byte[] body, Map<String, Object> headers) {
    this.body = body;
    this.headers = Maps.newHashMap();
    if (headers != null) {
      for (Map.Entry<String, Object> header : headers.entrySet()) {
        if (getValidPayloadHeaderValue(header.getValue())) {
          this.headers.put(header.getKey(), header.getValue());
        }
      }
    }
  }

  private byte[] body;

  private Map<String, Object> headers;

  private boolean getValidPayloadHeaderValue(Object headerValue) {
    if (headerValue instanceof String) {
      return true;
    } else if (headerValue instanceof BigDecimal) {
      return true;
    } else if (headerValue instanceof Number) {
      return true;
    } else if (headerValue instanceof Boolean) {
      return true;
    } else if (headerValue instanceof Date) {
      return true;
    } else if (headerValue instanceof byte[]) {
      return true;
    }
    return false;
  }
}
