package org.apache.camel.component.kafka;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pawas.kumar on 02/12/16.
 */
public class CamelKafkaGenericObject<A> implements Serializable{

  A body;

  Map<String, Object> headers;

  private CamelKafkaGenericObject() {
    this.headers = new HashMap<>();
  }

  public void setHeaders(Map<String, Object> headers) {
    if (headers != null) {
      headers.entrySet().stream()
          .filter(header -> getValidKafkaHeaderValue(header.getValue()))
          .forEach(header -> this.headers.put(header.getKey(), header.getValue()));
    }
  }

  public void setBody(A body) {
    this.body = body;
  }

  public A getBody() {
    return body;
  }

  public Map<String, Object> getHeaders() {
    return headers;
  }

  private boolean getValidKafkaHeaderValue(Object headerValue) {
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

  public static class CamelKafkaGenericObjectBuilder<A> {
    private A body;
    private Map<String, Object> headers;

    public CamelKafkaGenericObjectBuilder<A> setBody(A body) {
      this.body = body;
      return this;
    }

    public CamelKafkaGenericObjectBuilder<A> setHeaders(Map<String, Object> headers) {
      this.headers = headers;
      return this;
    }

    public CamelKafkaGenericObject<A> build() {
      CamelKafkaGenericObject<A> genericObject = new CamelKafkaGenericObject<>();
      genericObject.setBody(body);
      genericObject.setHeaders(headers);
      return genericObject;
    }
  }

}
