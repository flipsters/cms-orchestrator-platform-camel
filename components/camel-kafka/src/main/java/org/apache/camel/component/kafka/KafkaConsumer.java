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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Slf4j
public class KafkaConsumer extends DefaultConsumer {

    public static MetricRegistry metricRegistry = new MetricRegistry();
    static {
        final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        jmxReporter.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                jmxReporter.stop();
            }
        });
    }

    protected ExecutorService executor;
    private final KafkaEndpoint endpoint;
    private final Processor processor;
    private Map<ConsumerConnector, CyclicBarrier> consumerBarriers;

    public KafkaConsumer(KafkaEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        this.processor = processor;
        this.consumerBarriers = new HashMap<ConsumerConnector, CyclicBarrier>();
        if (endpoint.getZookeeperConnect() == null) {
            throw new IllegalArgumentException("zookeeper host or zookeeper connect must be specified");
        }
        if (endpoint.getGroupId() == null) {
            throw new IllegalArgumentException("groupId must not be null");
        }
    }

    Properties getProps() {
        Properties props = endpoint.getConfiguration().createConsumerProperties();
        props.put("zookeeper.connect", endpoint.getZookeeperConnect());
        props.put("group.id", endpoint.getGroupId());
        return props;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        log.info("Starting Kafka consumer");

        executor = endpoint.createExecutor();
        for (int i = 0; i < endpoint.getConsumersCount(); i++) {
            ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(getProps()));
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            topicCountMap.put(endpoint.getTopic(), endpoint.getConsumerStreams());
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(endpoint.getTopic());
            for (KafkaStream<byte[], byte[]> stream : streams) {
                KafkaConsumer.updatedKafkaStream(stream);
            }
            // commit periodically
            if (endpoint.isAutoCommitEnable() != null && !endpoint.isAutoCommitEnable()) {
                if ((endpoint.getConsumerTimeoutMs() == null || endpoint.getConsumerTimeoutMs() < 0)
                    && endpoint.getConsumerStreams() > 1) {
                    log.warn("consumerTimeoutMs is set to -1 (infinite) while requested multiple consumer streams.");
                }
                CyclicBarrier barrier = new CyclicBarrier(endpoint.getConsumerStreams(), new CommitOffsetTask(consumer));
                for (final KafkaStream<byte[], byte[]> stream : streams) {
                    executor.submit(new BatchingConsumerTask(stream, barrier));
                }
                consumerBarriers.put(consumer, barrier);
            } else {
                // auto commit
                for (final KafkaStream<byte[], byte[]> stream : streams) {
                    executor.submit(new AutoCommitConsumerTask(consumer, stream));
                }
                consumerBarriers.put(consumer, null);
            }
        }

    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        log.info("Stopping Kafka consumer");

        for (ConsumerConnector consumer : consumerBarriers.keySet()) {
            if (consumer != null) {
                consumer.shutdown();
            }
        }
        consumerBarriers.clear();

        if (executor != null) {
            if (getEndpoint() != null && getEndpoint().getCamelContext() != null) {
                getEndpoint().getCamelContext().getExecutorServiceManager().shutdownNow(executor);
            } else {
                executor.shutdownNow();
            }
        }
        executor = null;
    }

    class BatchingConsumerTask implements Runnable {

        private KafkaStream<byte[], byte[]> stream;
        private CyclicBarrier barrier;

        public BatchingConsumerTask(KafkaStream<byte[], byte[]> stream, CyclicBarrier barrier) {
            this.stream = stream;
            this.barrier = barrier;
        }

        public void run() {

            int processed = 0;
            boolean consumerTimeout;
            MessageAndMetadata<byte[], byte[]> mm;
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            boolean hasNext = true;
            while (hasNext) {
                long start = System.currentTimeMillis();
                try {
                    consumerTimeout = false;
                    // only poll the next message if we are allowed to run and are not suspending
                    if (isRunAllowed() && !isSuspendingOrSuspended() && it.hasNext()) {
                        mm = it.next();
                        metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".consume.meter").mark();
                        Exchange exchange = endpoint.createKafkaExchange(mm);
                        try {
                            processor.process(exchange);
                        } catch (Exception e) {
                            metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".consume.exception").mark();
                            metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.consume.exception").mark();
                            log.error("Failed to process message from topic {}", endpoint.getTopic(), e);
                        }
                        processed++;
                    } else {
                        // we don't need to process the message
                        hasNext = false;
                    }
                } catch (ConsumerTimeoutException e) {
                    log.trace("Consumer timeout occurred for topic {}", endpoint.getTopic(), e);
                    consumerTimeout = true;
                }

                if (processed >= endpoint.getBatchSize() || consumerTimeout
                    || (processed > 0 && !hasNext)) { // Need to commit the offset for the last round
                    try {
                        metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".barrierAwait.meter").mark();
                        long barrierAwaitStartTime = System.currentTimeMillis();
                        barrier.await(endpoint.getBarrierAwaitTimeoutMs(), TimeUnit.MILLISECONDS);
                        long barrierAwaitEndTime = System.currentTimeMillis();
                        metricRegistry.timer("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".barrierAwait.timer").update(barrierAwaitEndTime - barrierAwaitStartTime, TimeUnit.MILLISECONDS);
                        if (!consumerTimeout) {
                            processed = 0;
                        }
                    } catch (Exception e) {
                        log.error("Barrier await exception occured for topic {}", endpoint.getTopic(), e);
                        metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".barrierAwait.exception").mark();
                        metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.barrierAwait.exception").mark();
                        getExceptionHandler().handleException("Error waiting for batch to complete", e);
                        break;
                    }
                }
                long end = System.currentTimeMillis();
                if (!consumerTimeout) {
                    metricRegistry.timer("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".consume.timer").update(end - start, TimeUnit.MILLISECONDS);
                } else {
                    metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".consume.timeout.meter").mark();
                }
            }
        }
    }

    class CommitOffsetTask implements Runnable {

        private final ConsumerConnector consumer;

        public CommitOffsetTask(ConsumerConnector consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".commitOffsets.meter").mark();
                log.debug("Commit offsets on consumer: {} topic: {}", ObjectHelper.getIdentityHashCode(consumer), endpoint.getTopic());
                long start = System.currentTimeMillis();
                consumer.commitOffsets();
                long end = System.currentTimeMillis();
                metricRegistry.timer("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".commitOffsets.timer").update(end - start, TimeUnit.MILLISECONDS);
            } catch (RuntimeException e) {
                metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.topic." + endpoint.getTopic() + ".commitOffsets.exception").mark();
                metricRegistry.meter("org.apache.camel.component.kafka.KafkaConsumer.commitOffsets.exception").mark();
                log.error("Failed to commit offsets for topic {}", endpoint.getTopic(), e);
                throw e;
            }
        }
    }

    private static void updatedKafkaStream(KafkaStream<byte[], byte[]> stream) {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        try {
            Field valueDecoder = it.getClass().getDeclaredField("valueDecoder");
            valueDecoder.setAccessible(true);
            Decoder clientDecoder = (Decoder) valueDecoder.get(it);
            CamelKafkaExchangeDecoder customDecoder = new CamelKafkaExchangeDecoder();
            customDecoder.setClientDecoder(clientDecoder);
            valueDecoder.set(it, customDecoder);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.error(e.getMessage(), e);
        }
    }

    class AutoCommitConsumerTask implements Runnable {

        private final ConsumerConnector consumer;
        private KafkaStream<byte[], byte[]> stream;

        public AutoCommitConsumerTask(ConsumerConnector consumer, KafkaStream<byte[], byte[]> stream) {
            this.consumer = consumer;
            this.stream = stream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            // only poll the next message if we are allowed to run and are not suspending
            while (isRunAllowed() && !(isSuspended() || isSuspending()) && it.hasNext()) {
                MessageAndMetadata<byte[], byte[]> mm = it.next();
                Exchange exchange = endpoint.createKafkaExchange(mm);
                try {
                    processor.process(exchange);
                } catch (Exception e) {
                    getExceptionHandler().handleException("Error during processing", exchange, e);
                }
            }
            // no more data so commit offset
            log.debug("Commit offsets on consumer: {}", ObjectHelper.getIdentityHashCode(consumer));
            consumer.commitOffsets();
        }
    }
}

