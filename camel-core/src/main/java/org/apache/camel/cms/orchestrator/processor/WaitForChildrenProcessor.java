package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.utils.ByteUtils;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.processor.SendProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kartik.bommepally on 10/01/17.
 */
public class WaitForChildrenProcessor extends SendProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(WaitForChildrenProcessor.class);

    private Expression aggregatorIdExpression;
    private Expression endpointExpression;
    private AggregateStore aggregateStore;

    public WaitForChildrenProcessor(Expression aggregatorIdExpression, Expression endpointExpression, Endpoint destination) {
        super(destination);
        this.aggregatorIdExpression = aggregatorIdExpression;
        this.endpointExpression = endpointExpression;
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    public WaitForChildrenProcessor(Expression aggregatorIdExpression, Expression endpointExpression, Endpoint destination, ExchangePattern pattern) {
        super(destination, pattern);
        this.aggregatorIdExpression = aggregatorIdExpression;
        this.endpointExpression = endpointExpression;
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    @Override
    public String toString() {
        return "WaitForChildren(" + destination + (pattern != null ? " " + pattern : "") + ")";
    }

    private boolean preProcess(Exchange exchange) throws Exception {
        String requestId = PlatformUtils.getRequestId(exchange);
        String aggregatorId = aggregatorIdExpression.evaluate(exchange, String.class);
        String endpoint = endpointExpression.evaluate(exchange, String.class);
        Payload payload = new Payload(exchange.getIn().getBody(byte[].class), exchange.getIn().getHeaders());
        boolean isJoinable = aggregateStore.joinWithWait(requestId, endpoint, ByteUtils.getBytes(payload), aggregatorId);
        if (isJoinable) {
            LOG.info("Parent request ID is now joinable " + requestId);
            exchange.getIn().setBody(requestId.getBytes());
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
