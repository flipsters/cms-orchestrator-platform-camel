package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.cms.orchestrator.aggregator.TrackIdExtractor;
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
public class AsyncTrackProcessor extends RecipientList {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncTrackProcessor.class);

    private Expression callbackEndpointExpression;
    private Expression aggregatorIdExpression;
    private TrackIdExtractor trackIdExtractor;
    private RecipientList asyncCallbackRecipientList;
    private AggregateStore aggregateStore;

    public AsyncTrackProcessor(CamelContext camelContext, Expression expression, Expression callbackEndpointExpression,
                               Expression aggregatorIdExpression, TrackIdExtractor trackIdExtractor,
                               RecipientList asyncCallbackRecipientList, ExecutorService threadPool, boolean shutdownThreadPool,
                               RecipientList recipientList) {
        this(camelContext, expression, ",", callbackEndpointExpression, aggregatorIdExpression, trackIdExtractor,
                asyncCallbackRecipientList, threadPool, shutdownThreadPool, recipientList);
    }

    public AsyncTrackProcessor(CamelContext camelContext, Expression expression, String delimiter, Expression callbackEndpointExpression,
                               Expression aggregatorIdExpression, TrackIdExtractor trackIdExtractor, RecipientList asyncCallbackRecipientList,
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
        this.callbackEndpointExpression = callbackEndpointExpression;
        this.aggregatorIdExpression = aggregatorIdExpression;
        this.trackIdExtractor = trackIdExtractor;
        this.asyncCallbackRecipientList = asyncCallbackRecipientList;
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    @Override
    public void start() throws Exception {
        super.start();
        asyncCallbackRecipientList.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        asyncCallbackRecipientList.stop();
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        asyncCallbackRecipientList.shutdown();
    }

    @Override
    public String toString() {
        return "AsyncTrack(" + callbackEndpointExpression + ", " + aggregatorIdExpression + ", " + super.toString() + ")";
    }

    private boolean preProcess(String requestId, Payload originalPayload, Exchange exchange) throws Exception {
        LOG.info("Extracting track ID for request ID " + requestId);
        String trackId = trackIdExtractor.getTrackId(exchange);
        LOG.info("Obtained track ID " + trackId + " for request ID " + requestId);
        String aggregatorId = aggregatorIdExpression.evaluate(exchange, String.class);
        String callbackEndpoint = callbackEndpointExpression.evaluate(exchange, String.class);
        boolean isResumable = aggregateStore.createAsync(trackId, callbackEndpoint, ByteUtils.getBytes(originalPayload), aggregatorId);
        if (isResumable) {
            LOG.info("Track ID " + trackId + " with request ID " + requestId +  " is now resumable");
            exchange.getIn().setBody(trackId.getBytes());
        }
        return isResumable;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String requestId = PlatformUtils.getRequestId(exchange);
        Payload originalPayload = new Payload(exchange.getIn().getBody(byte[].class), exchange.getIn().getHeaders());
        super.process(exchange);
        boolean isResumable = preProcess(requestId, originalPayload, exchange);
        if (isResumable) {
            asyncCallbackRecipientList.process(exchange);
        }
    }

    @Override
    public boolean process(Exchange exchange, final AsyncCallback callback) {
        try {
            String requestId = PlatformUtils.getRequestId(exchange);
            Payload originalPayload = new Payload(exchange.getIn().getBody(byte[].class), exchange.getIn().getHeaders());
            super.process(exchange, callback);
            boolean isResumable = preProcess(requestId, originalPayload, exchange);
            if (isResumable) {
                asyncCallbackRecipientList.process(exchange);
            }
            return true;
        } catch (Exception e) {
            LOG.error("Failed to do async track for " + exchange.getIn().getHeaders(), e);
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }
}
