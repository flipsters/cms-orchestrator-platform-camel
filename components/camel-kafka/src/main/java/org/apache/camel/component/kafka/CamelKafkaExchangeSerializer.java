package org.apache.camel.component.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

@Slf4j
public class CamelKafkaExchangeSerializer implements Serializer<CamelKafkaExchangeObject> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, CamelKafkaExchangeObject object) {
        try {
            return getBytes(object);
        } catch (IOException e) {
            log.error("Error serializing message from topic {}", topic, e);
            throw new RuntimeException(e.getMessage(), e);
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

    @Override
    public void close() {
    }
}
