package org.apache.camel.component.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by kartik.bommepally on 24/04/17.
 */
public class ByteOrStringSerializer implements Serializer {

    @Override
    public void configure(Map configs, boolean isKey) {
        // nothing to do
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof String) {
            return ((String) data).getBytes();
        } else if (data instanceof byte[]) {
            return (byte[]) data;
        }
        throw new IllegalArgumentException("Data must be either String of byte[] but obtained type " + data.getClass() + " in topic " + topic);
    }

    @Override
    public void close() {
        // nothing to do
    }
}
