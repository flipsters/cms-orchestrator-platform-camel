package org.apache.camel.cms.orchestrator.processor;

import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.utils.ForkUtils;
import org.apache.camel.processor.RecipientList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class ForkProcessor extends RecipientList {

    private static final Logger LOG = LoggerFactory.getLogger(ForkProcessor.class);

    public ForkProcessor(CamelContext camelContext, Expression expression, ExecutorService threadPool,
                         boolean shutdownThreadPool, RecipientList recipientList) {
        this(camelContext, expression, ",", threadPool, shutdownThreadPool, recipientList);
    }

    public ForkProcessor(CamelContext camelContext, Expression expression, String delimiter, ExecutorService threadPool,
                         boolean shutdownThreadPool, RecipientList recipientList) {
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
    }

    @Override
    public String toString() {
        return "Fork(" + super.toString() + ")";
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        ForkUtils.createChild(exchange);
        super.process(exchange);
        ForkUtils.revertBackToParent(exchange);
    }

    @Override
    public boolean process(Exchange exchange, final AsyncCallback callback) {
        try {
            ForkUtils.createChild(exchange);
            boolean status = super.process(exchange, callback);
            ForkUtils.revertBackToParent(exchange);
            return status;
        } catch (Exception e) {
            LOG.error("Failed to do fork for " + exchange.getIn().getHeaders(), e);
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }
}
