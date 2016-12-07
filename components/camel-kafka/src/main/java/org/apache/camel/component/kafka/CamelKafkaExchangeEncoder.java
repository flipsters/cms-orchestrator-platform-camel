package org.apache.camel.component.kafka;

import kafka.serializer.Encoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class CamelKafkaExchangeEncoder<E extends Encoder<V>, V> implements Encoder<CamelKafkaGenericObject<V>> {

  E clientEncoder;

  public void setClientEncoder(E clientEncoder) {
    this.clientEncoder = clientEncoder;
  }

  @Override
  public byte[] toBytes(CamelKafkaGenericObject<V> vCamelKafkaGenericObject) {
    try {
      byte[] bytes = clientEncoder.toBytes(vCamelKafkaGenericObject.getBody());
      CamelKafkaGenericObject camelKafkaGenericObject =
          new CamelKafkaGenericObject.CamelKafkaGenericObjectBuilder<byte[]>()
              .setBody(bytes)
              .setHeaders(vCamelKafkaGenericObject.getHeaders())
              .build();
      return getBytes(camelKafkaGenericObject);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public byte[] getBytes(Object object) throws IOException {
    byte[] bytes = null;
    ByteArrayOutputStream bos = null;
    ObjectOutputStream oos = null;
    try {
      bos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bos);
      oos.writeObject(object);
      oos.flush();
      bytes = bos.toByteArray();
    } finally {
      if (oos != null) {
        oos.close();
      }
      if (bos != null) {
        bos.close();
      }
    }
    return bytes;
  }
}
