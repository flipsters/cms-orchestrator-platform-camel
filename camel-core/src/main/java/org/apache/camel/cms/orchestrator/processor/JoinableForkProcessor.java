package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.utils.ForkUtils;
import org.apache.camel.cms.orchestrator.exception.NoRequestIdPresentException;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.processor.SendProcessor;

import java.io.IOException;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class JoinableForkProcessor extends SendProcessor {

    protected AggregateStore aggregateStore;

    public JoinableForkProcessor(Endpoint destination) {
        super(destination);
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    public JoinableForkProcessor(Endpoint destination, ExchangePattern pattern){
        super(destination, pattern);
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    @Override
    public String toString() {
        return "JoinableFork(" + destination + (pattern != null ? " " + pattern : "") + ")";
    }

    private void preProcess(Exchange exchange) throws IOException, NoRequestIdPresentException {
        String routeId = ForkUtils.getRouteId(exchange);
        if (exchange.getProperty(OrchestratorConstants.IS_FIRST_FORK_PROPERTY) == null) {
            aggregateStore.clear(routeId, ForkUtils.getRequestId(exchange));
            exchange.setProperty(OrchestratorConstants.IS_FIRST_FORK_PROPERTY, true);
        }
        ForkUtils.createChild(exchange);
        aggregateStore.fork(ForkUtils.getParentRequestId(exchange), ForkUtils.getRequestId(exchange), routeId);
    }

    private void postProcess(Exchange exchange) throws NoRequestIdPresentException {
        ForkUtils.revertBackToParent(exchange);
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
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }

}
