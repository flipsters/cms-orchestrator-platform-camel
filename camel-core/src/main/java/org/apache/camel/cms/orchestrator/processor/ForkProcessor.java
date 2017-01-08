package org.apache.camel.cms.orchestrator.processor;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.exception.NoRequestIdPresentException;
import org.apache.camel.processor.SendProcessor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.UUID;

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
        createChild(exchange);
        super.process(exchange);
        revertBackToParent(exchange);
    }

    @Override
    public boolean process(Exchange exchange, final AsyncCallback callback) {
        try {
            createChild(exchange);
            boolean status = super.process(exchange, callback);
            revertBackToParent(exchange);
            return status;
        } catch (NoRequestIdPresentException e) {
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }

    private void revertBackToParent(Exchange exchange) {
        ArrayList<String> parentHierarchy = exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER,
                ArrayList.class);
        String requestId = parentHierarchy.get(parentHierarchy.size()-1);
        parentHierarchy.remove(parentHierarchy.size()-1);

        exchange.getIn().setHeader(OrchestratorConstants.REQUEST_ID_HEADER, requestId);
        exchange.getIn().setHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, parentHierarchy);
    }

    private void createChild(Exchange exchange) throws NoRequestIdPresentException {
        String childId = UUID.randomUUID().toString();
        String requestId = exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER, String.class);

        if(StringUtils.isEmpty(requestId)) {
            throw new NoRequestIdPresentException("Request Id absent!");
        }

        ArrayList<String> parentHierarchy = exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER,
                ArrayList.class);
        if(CollectionUtils.isEmpty(parentHierarchy)) {
            parentHierarchy = new ArrayList<>();
        }
        parentHierarchy.add(requestId);
        exchange.getIn().setHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, parentHierarchy);
        exchange.getIn().setHeader(OrchestratorConstants.REQUEST_ID_HEADER, childId);
    }
}
