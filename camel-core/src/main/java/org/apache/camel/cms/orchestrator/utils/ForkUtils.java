package org.apache.camel.cms.orchestrator.utils;

import org.apache.camel.Exchange;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.exception.NoRequestIdPresentException;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Stack;
import java.util.UUID;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class ForkUtils {

    public static void revertBackToParent(Exchange exchange) throws NoRequestIdPresentException {
        Stack<String> parentHierarchy = exchange.getIn()
                .getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, Stack.class);

        if(CollectionUtils.isEmpty(parentHierarchy)) {
            throw new NoRequestIdPresentException("Request Id absent!");
        }

        String requestId = parentHierarchy.pop();

        exchange.getIn().setHeader(OrchestratorConstants.REQUEST_ID_HEADER, requestId);
        exchange.getIn().setHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, parentHierarchy);
    }

    public static void createChild(Exchange exchange) throws NoRequestIdPresentException {
        String childId = UUID.randomUUID().toString();
        String requestId = exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER, String.class);
        Stack<String> parentHierarchy = exchange.getIn()
                .getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, Stack.class);

        if(StringUtils.isEmpty(requestId)) {
            throw new NoRequestIdPresentException("Request Id absent!");
        }

        if(CollectionUtils.isEmpty(parentHierarchy)) {
            parentHierarchy = new Stack<>();
        }
        parentHierarchy.push(requestId);

        exchange.getIn().setHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, parentHierarchy);
        exchange.getIn().setHeader(OrchestratorConstants.REQUEST_ID_HEADER, childId);
    }

    public static void storeForkState(Exchange exchange) throws NoRequestIdPresentException, IOException {
        Stack<String> parentHierarchy = exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, Stack.class);
        String childId = exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER, String.class);

        if(CollectionUtils.isEmpty(parentHierarchy)) {
            throw new NoRequestIdPresentException("No parent id present in child process!");
        }
        String parentId = parentHierarchy.peek();

        AggregateStoreFactory.getStoreInstance().fork(parentId, childId, exchange.getFromRouteId());
    }
}
