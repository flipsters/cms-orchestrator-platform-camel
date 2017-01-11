package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.processor.SendProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kartik.bommepally on 10/01/17.
 */
public class JoinProcessor extends SendProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(JoinProcessor.class);

    private Expression aggregatorIdExpression;
    private AggregateStore aggregateStore;

    public JoinProcessor(Expression aggregatorIdExpression, Endpoint destination) {
        super(destination);
        this.aggregatorIdExpression = aggregatorIdExpression;
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    public JoinProcessor(Expression aggregatorIdExpression, Endpoint destination, ExchangePattern pattern) {
        super(destination, pattern);
        this.aggregatorIdExpression = aggregatorIdExpression;
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    @Override
    public String toString() {
        return "Join(" + destination + (pattern != null ? " " + pattern : "") + ")";
    }

    private boolean preProcess(Exchange exchange) throws Exception {
        String requestId = PlatformUtils.getRequestId(exchange);
        String parentRequestId = PlatformUtils.getParentRequestId(exchange);
        String aggregatorId = aggregatorIdExpression.evaluate(exchange, String.class);
        byte[] payload = exchange.getIn().getBody(byte[].class);
        boolean isJoinable = aggregateStore.join(parentRequestId, requestId, payload, aggregatorId);
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
