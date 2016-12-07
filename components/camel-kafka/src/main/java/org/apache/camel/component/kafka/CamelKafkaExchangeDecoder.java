package org.apache.camel.component.kafka;

import kafka.serializer.Decoder;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Created by pawas.kumar on 05/12/16.
 */
public class CamelKafkaExchangeDecoder<D extends Decoder<V>, V>
    implements Decoder<CamelKafkaGenericObject<V>> {

  protected final org.slf4j.Logger log = LoggerFactory.getLogger(getClass());

  D clientDecoder;

  public void setClientDecoder(D clientDecoder) {
    this.clientDecoder = clientDecoder;
  }

  @Override
  public CamelKafkaGenericObject<V> fromBytes(byte[] bytes) {
    try {
      return getCamelKafkaGenericObject(bytes);
    } catch (IOException | ClassNotFoundException e) {
      log.error(e.getMessage(), e);
    }
    return new CamelKafkaGenericObject.CamelKafkaGenericObjectBuilder<V>()
        .build();
  }

  private CamelKafkaGenericObject<V> getCamelKafkaGenericObject(byte[] bytes)
      throws IOException, ClassNotFoundException {
    CamelKafkaGenericObject<V> obj = null;
    ByteArrayInputStream bis = null;
    ObjectInputStream ois = null;
    try {
      bis = new ByteArrayInputStream(bytes);
      ois = new ObjectInputStream(bis);
      CamelKafkaGenericObject<byte[]> camelKafkaGenericObject =
          (CamelKafkaGenericObject<byte[]>) ois.readObject();
      V body = clientDecoder.fromBytes(camelKafkaGenericObject.getBody());
      obj = new CamelKafkaGenericObject.CamelKafkaGenericObjectBuilder<V>()
          .setBody(body)
          .setHeaders(camelKafkaGenericObject.getHeaders())
          .build();
      obj.setHeaders(camelKafkaGenericObject.getHeaders());
      obj.setBody(body);
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
