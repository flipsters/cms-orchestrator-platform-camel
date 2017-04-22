package org.apache.camel.component.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * Created by pawas.kumar on 05/12/16.
 */
@Slf4j
public class CamelKafkaExchangeDeserializer implements Deserializer<CamelKafkaExchangeObject> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public CamelKafkaExchangeObject deserialize(String topic, byte[] bytes) {
        try {
            return getCamelKafkaGenericObject(bytes);
        } catch (IOException | ClassNotFoundException e) {
            log.error("Error deserializing message from topic {}", topic, e);
            throw new IllegalStateException(e);
        }
    }

    private CamelKafkaExchangeObject getCamelKafkaGenericObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bis);
            return (CamelKafkaExchangeObject) ois.readObject();
        } finally {
            if (bis != null) {
                bis.close();
            }
            if (ois != null) {
                ois.close();
            }
        }
    }

    @Override
    public void close() {
    }
}
