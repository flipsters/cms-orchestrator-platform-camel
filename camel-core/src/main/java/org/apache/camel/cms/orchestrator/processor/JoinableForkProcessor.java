package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.utils.ForkUtils;
import org.apache.camel.cms.orchestrator.exception.NoRequestIdPresentException;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformContext;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.processor.RecipientList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.apache.camel.cms.orchestrator.OrchestratorConstants.PARENT_REQUEST_ID_DELIM;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class JoinableForkProcessor extends RecipientList {

    private static final Logger LOG = LoggerFactory.getLogger(JoinableForkProcessor.class);
    private static final String LAST_FORK_CHILD_HEADER = "X-LastForkChildHeader";

    protected AggregateStore aggregateStore;

    public JoinableForkProcessor(CamelContext camelContext, Expression expression, ExecutorService threadPool,
                                 boolean shutdownThreadPool, RecipientList recipientList) {
        this(camelContext, expression, PARENT_REQUEST_ID_DELIM, threadPool, shutdownThreadPool, recipientList);
    }

    public JoinableForkProcessor(CamelContext camelContext, Expression expression, String delimiter,
                                 ExecutorService threadPool, boolean shutdownThreadPool, RecipientList recipientList) {
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
        this.aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    @Override
    public String toString() {
        return "JoinableFork(" + super.toString() + ")";
    }

    private void preProcess(Exchange exchange) throws IOException, NoRequestIdPresentException {
        String routeId = ForkUtils.getRouteId(exchange);
        // =================== Relevant only for first fork of the route clean up
        PlatformContext platformContext = exchange.getProperty(OrchestratorConstants.PLATFORM_CONTEXT_PROPERTY, PlatformContext.class);
        synchronized (platformContext) {
            if (platformContext.isRoutesFirstFork()) {
                aggregateStore.clear(ForkUtils.getRequestId(exchange), routeId);
                platformContext.markForkDone();
            }
        }
        // ===================
        String childId = exchange.getUnitOfWork().getOriginalInMessage().getHeader(LAST_FORK_CHILD_HEADER, String.class);
        if (childId != null) {
            aggregateStore.clear(ForkUtils.getRequestId(exchange), childId, routeId);
        }
        ForkUtils.createChild(exchange);
        childId = PlatformUtils.getRequestId(exchange);
        exchange.getUnitOfWork().getOriginalInMessage().setHeader(LAST_FORK_CHILD_HEADER, childId);
        aggregateStore.fork(ForkUtils.getParentRequestId(exchange), ForkUtils.getRequestId(exchange), routeId);
    }

    private void postProcess(Exchange exchange) throws NoRequestIdPresentException {
        ForkUtils.revertBackToParent(exchange);
        exchange.getUnitOfWork().getOriginalInMessage().removeHeader(LAST_FORK_CHILD_HEADER);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        preProcess(exchange);
        super.process(exchange);
        postProcess(exchange);
    }

    @Override
    public boolean process(Exchange exchange, final AsyncCallback callback) {
        try {
            preProcess(exchange);
            boolean status = super.process(exchange, callback);
            postProcess(exchange);
            return status;
        } catch (Exception e) {
            LOG.error("Failed to do joinable fork for " + exchange.getIn().getHeaders(), e);
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }

}
