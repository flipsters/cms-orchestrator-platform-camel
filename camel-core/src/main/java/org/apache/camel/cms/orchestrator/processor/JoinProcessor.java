package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.utils.ByteUtils;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.processor.RecipientList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * Created by kartik.bommepally on 10/01/17.
 */
public class JoinProcessor extends RecipientList {

    private static final Logger LOG = LoggerFactory.getLogger(JoinProcessor.class);

    private String aggregatorId;
    private AggregateStore aggregateStore;

    public JoinProcessor(CamelContext camelContext, Expression expression, String aggregatorId, ExecutorService threadPool,
                                 boolean shutdownThreadPool, RecipientList recipientList) {
        this(camelContext, expression, ",", aggregatorId, threadPool, shutdownThreadPool, recipientList);
    }

    public JoinProcessor(CamelContext camelContext, Expression expression, String delimiter, String aggregatorId,
                                 ExecutorService threadPool, boolean shutdownThreadPool, RecipientList recipientList) {
        super(camelContext, expression, delimiter);
        setAggregationStrategy(recipientList.getAggregationStrategy());
        setParallelProcessing(recipientList.isParallelProcessing());
        setParallelAggregate(recipientList.isParallelAggregate());
        setStreaming(recipientList.isStreaming());
        setShareUnitOfWork(recipientList.isShareUnitOfWork());
        setStopOnException(recipientList.isStopOnException());
        setIgnoreInvalidEndpoints(recipientList.isIgnoreInvalidEndpoints());
        setCacheSize(recipientList.getCacheSize());
        setOnPrepare(recipientList.getOnPrepare());
        setTimeout(recipientList.getTimeout());
        setExecutorService(threadPool);
        setShutdownExecutorService(shutdownThreadPool);
        this.aggregatorId = aggregatorId;
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    @Override
    public String toString() {
        return "Join(" + aggregatorId + ", " + super.toString() + ")";
    }

    private boolean preProcess(Exchange exchange) throws Exception {
        String requestId = PlatformUtils.getRequestId(exchange);
        String parentRequestId = PlatformUtils.getParentRequestId(exchange);
        Payload payload = new Payload(exchange.getIn().getBody(byte[].class), exchange.getIn().getHeaders());
        boolean isJoinable = aggregateStore.join(parentRequestId, requestId, ByteUtils.getBytes(payload), aggregatorId);
        if (isJoinable) {
            LOG.info("Parent request ID is now joinable " + parentRequestId);
            exchange.getIn().setBody(parentRequestId.getBytes());
        }
        return isJoinable;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        boolean isJoinable = preProcess(exchange);
        if (isJoinable) {
            super.process(exchange);
        }
    }

    @Override
    public boolean process(Exchange exchange, final AsyncCallback callback) {
        try {
            boolean isJoinable = preProcess(exchange);
            if (isJoinable) {
                boolean status = super.process(exchange, callback);
                return status;
            }
            return true;
        } catch (Exception e) {
            LOG.error("Failed to do join for " + exchange.getIn().getHeaders(), e);
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }
}
