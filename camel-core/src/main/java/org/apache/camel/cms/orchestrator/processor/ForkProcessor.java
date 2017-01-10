package org.apache.camel.cms.orchestrator.processor;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.cms.orchestrator.utils.ForkUtils;
import org.apache.camel.cms.orchestrator.exception.NoRequestIdPresentException;
import org.apache.camel.processor.SendProcessor;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class ForkProcessor extends SendProcessor {

    public ForkProcessor(Endpoint destination) {
        super(destination);
    }

    public ForkProcessor(Endpoint destination, ExchangePattern pattern){
        super(destination, pattern);
    }

    @Override
    public String toString() {
        return "Fork(" + destination + (pattern != null ? " " + pattern : "") + ")";
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
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }


}
