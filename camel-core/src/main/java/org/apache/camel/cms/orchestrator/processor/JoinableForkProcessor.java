package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.cms.orchestrator.ForkUtils;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.exception.NoRequestIdPresentException;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.processor.SendProcessor;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.util.Stack;

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

    @Override
    public void process(Exchange exchange) throws Exception {
        ForkUtils.createChild(exchange);
        ForkUtils.storeForkState(exchange);
        super.process(exchange);
        ForkUtils.revertBackToParent(exchange);
    }

    @Override
    public boolean process(Exchange exchange, final AsyncCallback callback) {
        try {
            ForkUtils.createChild(exchange);
            ForkUtils.storeForkState(exchange);
            boolean status = super.process(exchange, callback);
            ForkUtils.revertBackToParent(exchange);
            return status;
        } catch (NoRequestIdPresentException|IOException e) {
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }

}
