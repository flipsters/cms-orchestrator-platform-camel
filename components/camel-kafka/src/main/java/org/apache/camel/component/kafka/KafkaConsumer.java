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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.util.IOHelper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import static org.apache.camel.component.kafka.KafkaEndpoint.METRIC_REGISTRY;

@Slf4j
public class KafkaConsumer extends DefaultConsumer {

    protected ExecutorService executor;
    private final KafkaEndpoint endpoint;
    private final Processor processor;
    private final Long pollTimeoutMs;
    // This list helps working around the infinite loop of KAFKA-1894
    private final List<KafkaFetchRecords> tasks = new ArrayList<>();

    public KafkaConsumer(KafkaEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.processor = processor;
        this.pollTimeoutMs = endpoint.getConfiguration().getPollTimeoutMs();

        if (endpoint.getConfiguration().getBrokers() == null) {
            throw new IllegalArgumentException("BootStrap servers must be specified");
        }
        if (endpoint.getConfiguration().getGroupId() == null) {
            throw new IllegalArgumentException("groupId must not be null");
        }
    }

    Properties getProps() {
        Properties props = endpoint.getConfiguration().createConsumerProperties();
        endpoint.updateClassProperties(props);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint.getConfiguration().getBrokers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, endpoint.getConfiguration().getGroupId());
        return props;
    }

    @Override
    protected void doStart() throws Exception {
        log.info("Starting Kafka consumer");
        super.doStart();

        executor = endpoint.createExecutor();
        for (int i = 0; i < endpoint.getConfiguration().getConsumersCount(); i++) {
            KafkaFetchRecords task = new KafkaFetchRecords(endpoint.getConfiguration().getTopic(), i + "", getProps());
            executor.submit(task);
            tasks.add(task);
        }
    }

    // TODO Add more strict try / catches to be very careful
    @Override
    protected void doStop() throws Exception {
        log.info("Stopping Kafka consumer");

        if (executor != null) {
            if (getEndpoint() != null && getEndpoint().getCamelContext() != null) {
                getEndpoint().getCamelContext().getExecutorServiceManager().shutdownGraceful(executor);
            } else {
                executor.shutdownNow();
            }
            if (!executor.isTerminated()) {
                for (KafkaFetchRecords task : tasks) {
                    task.shutdown();
                }
                executor.shutdownNow();
            }
        }
        tasks.clear();
        executor = null;

        super.doStop();
    }

    class KafkaFetchRecords implements Runnable {

        private final org.apache.kafka.clients.consumer.KafkaConsumer consumer;
        private final String topicName;
        private final String threadId;
        private final Properties kafkaProps;

        KafkaFetchRecords(String topicName, String id, Properties kafkaProps) {
            this.topicName = topicName;
            this.threadId = topicName + "-" + "Thread " + id;
            this.kafkaProps = kafkaProps;

            ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                // Kafka uses reflection for loading authentication settings, use its classloader
                Thread.currentThread().setContextClassLoader(org.apache.kafka.clients.consumer.KafkaConsumer.class.getClassLoader());
                this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(kafkaProps, new ByteArrayDeserializer(), new CamelKafkaExchangeDeserializer());
            } finally {
                Thread.currentThread().setContextClassLoader(threadClassLoader);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            try {
                log.info("Subscribing {} to topic {}", threadId, topicName);
                consumer.subscribe(Arrays.asList(topicName.split(",")));

                if (endpoint.getConfiguration().isSeekToBeginning()) {
                    log.debug("{} is seeking to the beginning on topic {}", threadId, topicName);
                    // This poll to ensures we have an assigned partition otherwise seek won't work
                    consumer.poll(100);
                    consumer.seekToBeginning(consumer.assignment());
                }
                while (isRunAllowed() && !isStoppingOrStopped() && !isSuspendingOrSuspended()) {
                    ConsumerRecords<Object, Object> allRecords = consumer.poll(pollTimeoutMs);
                    for (TopicPartition partition : allRecords.partitions()) {
                        List<ConsumerRecord<Object, Object>> partitionRecords = allRecords.records(partition);
                        for (ConsumerRecord<Object, Object> record : partitionRecords) {
                            long start = System.currentTimeMillis();
                            METRIC_REGISTRY.meter("org.apache.camel.kafka.component.KafkaConsumer.consume.meter").mark();
                            METRIC_REGISTRY.meter("org.apache.camel.kafka.component.KafkaConsumer.topic." + topicName + ".consume.meter").mark();
                            log.trace("partition = {}, offset = {}, key = {}, value = {}", new Object[] {record.partition(), record.offset(), record.key(), record.value()});
                            Exchange exchange = endpoint.createKafkaExchange(record);
                            processor.process(exchange);
                            long end = System.currentTimeMillis();
                            METRIC_REGISTRY.timer("org.apache.camel.kafka.component.KafkaConsumer.consume.time").update(end - start, TimeUnit.MILLISECONDS);
                            METRIC_REGISTRY.timer("org.apache.camel.kafka.component.KafkaConsumer.topic." + topicName + ".consume.time").update(end - start, TimeUnit.MILLISECONDS);
                        }
                        // if autocommit is false
                        if (endpoint.getConfiguration().isAutoCommitEnable() != null && !endpoint.getConfiguration().isAutoCommitEnable()) {
                            METRIC_REGISTRY.meter("org.apache.camel.kafka.component.KafkaConsumer.commitOffset.meter").mark();
                            METRIC_REGISTRY.meter("org.apache.camel.kafka.component.KafkaConsumer.topic." + topicName + ".commitOffset.meter").mark();
                            long start = System.currentTimeMillis();
                            long partitionLastoffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(partitionLastoffset + 1)));
                            long end = System.currentTimeMillis();
                            METRIC_REGISTRY.timer("org.apache.camel.kafka.component.KafkaConsumer.commitOffset.time").update(end - start, TimeUnit.MILLISECONDS);
                            METRIC_REGISTRY.timer("org.apache.camel.kafka.component.KafkaConsumer.topic." + topicName + ".commitOffset.timer").update(end - start, TimeUnit.MILLISECONDS);
                        }
                    }
                }
                log.info("Unsubscribing " + threadId + " from topic " + topicName);
                consumer.unsubscribe();
            } catch (InterruptException e) {
                getExceptionHandler().handleException("Interrupted while consuming " + threadId + " from kafka topic " + topicName, e);
                log.error("Interrupted consuming " + threadId + " from topic " + topicName);
                consumer.unsubscribe();
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                METRIC_REGISTRY.meter("org.apache.camel.kafka.component.KafkaConsumer.consume.exception").mark();
                METRIC_REGISTRY.meter("org.apache.camel.kafka.component.KafkaConsumer.topic." + topicName + ".consume.exception").mark();
                log.error("Error consuming " + threadId + " from topic " + topicName, e);
                getExceptionHandler().handleException("Error consuming " + threadId + " from kafka topic " + topicName, e);
                consumer.unsubscribe();
            } finally {
                log.debug("Closing {} ", threadId);
                IOHelper.close(consumer);
            }
        }

        private void shutdown() {
            // As advised in the KAFKA-1894 ticket, calling this wakeup method breaks the infinite loop
            consumer.wakeup();
        }
    }

}

