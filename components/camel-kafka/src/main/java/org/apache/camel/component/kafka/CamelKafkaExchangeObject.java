package org.apache.camel.component.kafka;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

public class CamelKafkaExchangeObject implements Serializable {

    private byte[] body;

    private Map<String, Object> headers;

    private CamelKafkaExchangeObject() {
        this.headers = Maps.newHashMap();
    }

    public void setHeaders(Map<String, Object> headers) {
        if (headers != null) {
            for (Map.Entry<String, Object> header : headers.entrySet()) {
                if (getValidKafkaHeaderValue(header.getValue())) {
                    this.headers.put(header.getKey(), header.getValue());
                }
            }
        }
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
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

    public static class CamelKafkaExchangeObjectBuilder {
        private byte[] body;
        private Map<String, Object> headers;

        public CamelKafkaExchangeObjectBuilder setBody(byte[] body) {
            this.body = body;
            return this;
        }

        public CamelKafkaExchangeObjectBuilder setHeaders(Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        public CamelKafkaExchangeObject build() {
            CamelKafkaExchangeObject genericObject = new CamelKafkaExchangeObject();
            genericObject.setBody(body);
            genericObject.setHeaders(headers);
            return genericObject;
        }
    }
}
