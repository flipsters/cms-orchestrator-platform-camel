package org.apache.camel.cms.orchestrator.aggregator;

import com.google.common.collect.Maps;
import lombok.Data;
import lombok.NoArgsConstructor;

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
@Data
@NoArgsConstructor
public class Payload<A> implements Serializable {

  public Payload(A body, Map<String, Object> headers) {
    this.body = body;
    this.headers = Maps.newHashMap();
    this.bodyType = body.getClass();
    if (headers != null) {
      for (Map.Entry<String, Object> header : headers.entrySet()) {
        if (getValidPayloadHeaderValue(header.getValue())) {
          this.headers.put(header.getKey(), header.getValue());
        }
      }
    }
  }

  public Payload(A body, Map<String, Object> headers, Class bodyType) {
    this.body = body;
    this.headers = Maps.newHashMap();
    this.bodyType = bodyType;
    if (headers != null) {
      for (Map.Entry<String, Object> header : headers.entrySet()) {
        if (getValidPayloadHeaderValue(header.getValue())) {
          this.headers.put(header.getKey(), header.getValue());
        }
      }
    }
  }

  private A body;

  private Map<String, Object> headers;

  private Class bodyType;

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
