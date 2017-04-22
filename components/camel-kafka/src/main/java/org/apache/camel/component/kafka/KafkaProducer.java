/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.AsyncCallback;
import org.apache.camel.CamelException;
import org.apache.camel.CamelExchangeException;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import static org.apache.camel.component.kafka.KafkaEndpoint.METRIC_REGISTRY;

@Slf4j
public class KafkaProducer extends DefaultAsyncProducer {

    private org.apache.kafka.clients.producer.KafkaProducer kafkaProducer;
    private final KafkaEndpoint endpoint;

    public KafkaProducer(KafkaEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    Properties getProps() {
        Properties props = endpoint.getConfiguration().createProducerProperties();
        endpoint.updateClassProperties(props);
        if (endpoint.getConfiguration().getBrokers() != null) {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint.getConfiguration().getBrokers());
        }
        return props;
    }


    public org.apache.kafka.clients.producer.KafkaProducer getKafkaProducer() {
        return kafkaProducer;
    }

    /**
     * To use a custom {@link org.apache.kafka.clients.producer.KafkaProducer} instance.
     */
    public void setKafkaProducer(org.apache.kafka.clients.producer.KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    protected void doStart() throws Exception {
        Properties props = getProps();
        if (kafkaProducer == null) {
            ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                // Kafka uses reflection for loading authentication settings, use its classloader
                Thread.currentThread().setContextClassLoader(org.apache.kafka.clients.producer.KafkaProducer.class.getClassLoader());
                kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer(props, new ByteArraySerializer(), new CamelKafkaExchangeSerializer());
            } finally {
                Thread.currentThread().setContextClassLoader(threadClassLoader);
            }
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

    @SuppressWarnings("unchecked")
    protected Iterator<ProducerRecord> createRecorder(Exchange exchange) throws CamelException {
        String topic = endpoint.getConfiguration().getTopic();
        if (!endpoint.isBridgeEndpoint()) {
            topic = exchange.getIn().getHeader(KafkaConstants.TOPIC, topic, String.class);
        }
        if (topic == null) {
            throw new CamelExchangeException("No topic key set", exchange);
        }
        Object partitionKey = exchange.getIn().getHeader(KafkaConstants.PARTITION_KEY);
        boolean hasPartitionKey = false;
        Integer partitionKeyInt = null;
        if (partitionKey != null) {
            try {
                partitionKeyInt = new Integer(partitionKey.toString());
                hasPartitionKey = true;
            } catch (NumberFormatException e) {
            }
        }

        Object messageKey = exchange.getIn().getHeader(KafkaConstants.KEY);
        if (!hasPartitionKey && messageKey == null) {
            messageKey = partitionKey;
        }
        boolean hasMessageKey = messageKey != null;

        byte[] bytes = exchange.getIn().getBody(byte[].class);
        CamelKafkaExchangeObject object = new CamelKafkaExchangeObject.CamelKafkaExchangeObjectBuilder().setBody(bytes).setHeaders(exchange.getIn().getHeaders()).build();
        ProducerRecord record;
        if (hasPartitionKey && hasMessageKey) {
            record = new ProducerRecord(topic, partitionKeyInt, messageKey, object);
        } else if (hasMessageKey) {
            record = new ProducerRecord(topic, messageKey, object);
        } else {
            log.warn("No message key or partition key set");
            record = new ProducerRecord(topic, object);
        }
        return Collections.singletonList(record).iterator();
    }

    @Override
    @SuppressWarnings("unchecked")
    // Camel calls this method if the endpoint isSynchronous(), as the KafkaEndpoint creates a SynchronousDelegateProducer for it
    public void process(Exchange exchange) throws Exception {
        Iterator<ProducerRecord> c = createRecorder(exchange);
        List<Future<RecordMetadata>> futures = new LinkedList<Future<RecordMetadata>>();
        List<RecordMetadata> recordMetadatas = new ArrayList<RecordMetadata>();

        if (endpoint.getConfiguration().isRecordMetadata()) {
            if (exchange.hasOut()) {
                exchange.getOut().setHeader(KafkaConstants.KAFKA_RECORDMETA, recordMetadatas);
            } else {
                exchange.getIn().setHeader(KafkaConstants.KAFKA_RECORDMETA, recordMetadatas);
            }
        }

        String topic = endpoint.getConfiguration().getTopic();
        METRIC_REGISTRY.meter("org.apache.camel.component.kafka.KafkaProducer.send.meter").mark();
        METRIC_REGISTRY.meter("org.apache.camel.component.kafka.KafkaProducer.topic." + topic + ".send.meter").mark();
        long start = System.currentTimeMillis();
        try {
            while (c.hasNext()) {
                futures.add(kafkaProducer.send(c.next()));
            }
            for (Future<RecordMetadata> f : futures) {
                //wait for them all to be sent
                recordMetadatas.add(f.get());
            }
        } catch (Exception e) {
            METRIC_REGISTRY.meter("org.apache.camel.component.kafka.KafkaProducer.send.exception").mark();
            METRIC_REGISTRY.meter("org.apache.camel.component.kafka.KafkaProducer.topic." + topic + ".send.exception").mark();
            throw e;
        } finally {
            long end = System.currentTimeMillis();
            METRIC_REGISTRY.timer("org.apache.camel.component.kafka.KafkaProducer.topic." + topic + ".send.timer").update(end - start, TimeUnit.MILLISECONDS);
            METRIC_REGISTRY.timer("org.apache.camel.component.kafka.KafkaProducer.send.timer").update(end - start, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean process(Exchange exchange, AsyncCallback callback) {
        try {
            process(exchange);
        } catch (Exception ex) {
            exchange.setException(ex);
        }
        callback.done(true);
        return true;
    }
}
