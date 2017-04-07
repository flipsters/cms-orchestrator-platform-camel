package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import flipkart.cms.aggregator.model.JoinResponse;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.utils.ByteUtils;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.processor.RecipientList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import static org.apache.camel.cms.orchestrator.OrchestratorConstants.PARENT_REQUEST_ID_DELIM;
import static org.apache.camel.cms.orchestrator.OrchestratorConstants.ROUTE_JOIN_ID;

/**
 * Created by kartik.bommepally on 10/01/17.
 */
public class WaitForChildrenProcessor extends org.apache.camel.processor.RecipientList {

    private static final Logger LOG = LoggerFactory.getLogger(WaitForChildrenProcessor.class);

    private Expression aggregatorIdExpression;
    private Expression callbackEndpointExpression;
    private AggregateStore aggregateStore;

    public WaitForChildrenProcessor(CamelContext camelContext, Expression expression, Expression aggregatorIdExpression, Expression callbackEndpointExpression,
                                    ExecutorService threadPool, boolean shutdownThreadPool, RecipientList recipientList) {
        this(camelContext, expression, PARENT_REQUEST_ID_DELIM, aggregatorIdExpression, callbackEndpointExpression, threadPool, shutdownThreadPool, recipientList);
    }

    public WaitForChildrenProcessor(CamelContext camelContext, Expression expression, String delimiter, Expression aggregatorIdExpression,
                                    Expression callbackEndpointExpression, ExecutorService threadPool, boolean shutdownThreadPool, RecipientList recipientList) {
        super(camelContext, expression, delimiter);
        setAggregationStrategy(recipientList.getAggregationStrategy());
        setParallelProcessing(recipientList.isParallelProcessing());
        setParallelAggregate(recipientList.isParallelAggregate());
        setStreaming(recipientList.isStreaming());
        setShareUnitOfWork(true); // Force setting
        setStopOnException(recipientList.isStopOnException());
        setIgnoreInvalidEndpoints(recipientList.isIgnoreInvalidEndpoints());
        setCacheSize(recipientList.getCacheSize());
        setOnPrepare(recipientList.getOnPrepare());
        setTimeout(recipientList.getTimeout());
        setExecutorService(threadPool);
        setShutdownExecutorService(shutdownThreadPool);
        this.aggregatorIdExpression = aggregatorIdExpression;
        this.callbackEndpointExpression = callbackEndpointExpression;
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    @Override
    public String toString() {
        return "Join(" + aggregatorIdExpression + ", " + callbackEndpointExpression + ", " + super.toString() + ")";
    }

    private boolean preProcess(Exchange exchange) throws Exception {
        String requestId = PlatformUtils.getRequestId(exchange);
        Payload payload = ByteUtils.createPayload(exchange);
        String aggregatorId = aggregatorIdExpression.evaluate(exchange, String.class);
        String callbackEndpoint = callbackEndpointExpression.evaluate(exchange, String.class);
        byte[] rawPayload = ByteUtils.getByteArrayFromPayload(getCamelContext().getTypeConverterRegistry(), payload);
        JoinResponse joinResponse = aggregateStore.joinWithWait(requestId, callbackEndpoint, rawPayload, aggregatorId, exchange.getFromRouteId());
        boolean isJoinable = joinResponse.isJoinable();
        if (isJoinable) {
            LOG.info("Parent request ID is now joinable " + requestId);
            exchange.getIn().setBody(requestId.getBytes());
            exchange.getIn().setHeader(ROUTE_JOIN_ID, joinResponse.getRouteId());
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
