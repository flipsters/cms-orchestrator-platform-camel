package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import flipkart.cms.aggregator.client.ExpiryStore;
import flipkart.cms.aggregator.client.MappingStore;
import flipkart.cms.aggregator.model.ExpiryEntry;
import flipkart.cms.aggregator.model.RequestMap;
import org.apache.camel.AsyncCallback;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Expression;
import org.apache.camel.cms.orchestrator.aggregator.AsyncAckExtractor;
import org.apache.camel.cms.orchestrator.aggregator.CallbackUrlAppender;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.cms.orchestrator.aggregator.RequestIdentifier;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.factory.ExpiryStoreFactory;
import org.apache.camel.cms.orchestrator.factory.MappingStoreFactory;
import org.apache.camel.cms.orchestrator.utils.ByteUtils;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.processor.RecipientList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import static org.apache.camel.cms.orchestrator.OrchestratorConstants.PARENT_REQUEST_ID_DELIM;

/**
 * Created by kartik.bommepally on 10/01/17.
 */
public class AsyncTrackProcessor extends RecipientList {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncTrackProcessor.class);

    private Expression callbackEndpointExpression;
    private Expression aggregatorIdExpression;
    private AsyncAckExtractor asyncAckExtractor;
    private CallbackUrlAppender callbackUrlAppender;
    private RecipientList asyncCallbackRecipientList;
    private AggregateStore aggregateStore;
    private MappingStore mappingStore;
    private ExpiryStore expiryStore;
    private Long expiryBreachTime;

    public AsyncTrackProcessor(CamelContext camelContext, Expression expression, Expression callbackEndpointExpression,
                               Expression aggregatorIdExpression, CallbackUrlAppender callbackUrlAppender, AsyncAckExtractor asyncAckExtractor,
                               RecipientList asyncCallbackRecipientList, ExecutorService threadPool, boolean shutdownThreadPool,
                               RecipientList recipientList, Long expiryBreachTime) {
        this(camelContext, expression, PARENT_REQUEST_ID_DELIM, callbackEndpointExpression, aggregatorIdExpression, callbackUrlAppender, asyncAckExtractor,
                asyncCallbackRecipientList, threadPool, shutdownThreadPool, recipientList, expiryBreachTime);
    }

    public AsyncTrackProcessor(CamelContext camelContext, Expression expression, String delimiter, Expression callbackEndpointExpression,
                               Expression aggregatorIdExpression, CallbackUrlAppender callbackUrlAppender, AsyncAckExtractor asyncAckExtractor, RecipientList asyncCallbackRecipientList,
                               ExecutorService threadPool, boolean shutdownThreadPool, RecipientList recipientList, Long expiryBreachTime) {
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
        this.callbackEndpointExpression = callbackEndpointExpression;
        this.aggregatorIdExpression = aggregatorIdExpression;
        this.asyncAckExtractor = asyncAckExtractor;
        this.callbackUrlAppender = callbackUrlAppender;
        this.asyncCallbackRecipientList = asyncCallbackRecipientList;
        this.expiryBreachTime = expiryBreachTime;
        aggregateStore = AggregateStoreFactory.getStoreInstance();
        mappingStore = MappingStoreFactory.getStoreInstance();
        expiryStore = ExpiryStoreFactory.getStoreInstance(expiryBreachTime);
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

    private boolean postProcess(String requestId, Payload originalPayload, Exchange exchange, String trackId, String tenantId) throws Exception {
        LOG.info("Extracting track ID for request ID " + requestId);
        RequestIdentifier requestIdentifier = asyncAckExtractor.getRequestIdentifier(exchange);
        String externalRequestId = requestIdentifier.getRequestId();
        String externalTenantId = requestIdentifier.getTenantId();
        // call map table and store the information over there
        LOG.info("Obtained track ID " + trackId + " for request ID " + requestId);
        String aggregatorId = aggregatorIdExpression.evaluate(exchange, String.class);
        String callbackEndpoint = callbackEndpointExpression.evaluate(exchange, String.class);
        byte[] rawPayload = ByteUtils.getByteArrayFromPayload(getCamelContext().getTypeConverterRegistry(), originalPayload);
        boolean isResumable = aggregateStore.createAsync(trackId, callbackEndpoint, rawPayload, aggregatorId);
        mappingStore.putRequestMap(new RequestMap(requestId, externalTenantId, externalRequestId, trackId));
        if (isResumable) {
            LOG.info("Track ID " + trackId + " with request ID " + requestId +  " is now resumable");
            exchange.getIn().setBody(trackId.getBytes());
        }
        return isResumable;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String requestId = PlatformUtils.getRequestId(exchange);
        String tentantId = PlatformUtils.getTenantId(exchange);
        Payload originalPayload = ByteUtils.createPayload(exchange);
        String trackId = callbackUrlAppender.mergeCallback(exchange);
        ExpiryEntry expiryEntry = new ExpiryEntry(expiryBreachTime, trackId, requestId);
        expiryStore.putExpiry(expiryEntry);
        super.process(exchange);
        if (exchange.getException() == null) {
            boolean isResumable = postProcess(requestId, originalPayload, exchange, trackId, tentantId);
            if (isResumable) {
                asyncCallbackRecipientList.process(exchange);
            }
        }
    }

    @Override
    public boolean process(Exchange exchange, final AsyncCallback callback) {
        try {
            String requestId = PlatformUtils.getRequestId(exchange);
            String tentantId = PlatformUtils.getTenantId(exchange);
            Payload originalPayload = ByteUtils.createPayload(exchange);
            String trackId = callbackUrlAppender.mergeCallback(exchange);
            boolean process = super.process(exchange, callback);
            if (exchange.getException() == null) {
                boolean isResumable = postProcess(requestId, originalPayload, exchange, trackId, tentantId);
                if (isResumable) {
                    asyncCallbackRecipientList.process(exchange);
                }
            }
            return process;
        } catch (Exception e) {
            LOG.error("Failed to do async track for " + exchange.getIn().getHeaders(), e);
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }
}
