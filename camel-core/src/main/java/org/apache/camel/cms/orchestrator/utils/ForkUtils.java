package org.apache.camel.cms.orchestrator.utils;

import org.apache.camel.Exchange;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.exception.NoRequestIdPresentException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.UUID;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class ForkUtils {

    private static final String ROUTE_ID_NOT_DEFINED = "ROUTE_ID_NOT_DEFINED";

    public static String getRequestId(Exchange exchange) {
        return exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER, String.class);
    }

    private static String[] getParentRequestIdStack(Exchange exchange) {
        String parentIdStack = exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, String.class);
        if (StringUtils.isEmpty(parentIdStack)) {
            return null;
        }
        return parentIdStack.split(OrchestratorConstants.PARENT_REQUEST_ID_DELIM);
    }

    public static String getParentRequestId(Exchange exchange) {
        String[] parentRequestIdStack = getParentRequestIdStack(exchange);
        return (parentRequestIdStack == null) ? null : parentRequestIdStack[0];
    }

    public static void revertBackToParent(Exchange exchange) throws NoRequestIdPresentException {
        String[] parentRequestIdStack = getParentRequestIdStack(exchange);
        if (parentRequestIdStack == null) {
            throw new NoRequestIdPresentException("Parent request ID absent!");
        }
        String requestId = parentRequestIdStack[0];
        exchange.getIn().setHeader(OrchestratorConstants.REQUEST_ID_HEADER, requestId);
        String parentRequestIdStackStr = null;
        if (parentRequestIdStack.length > 1) {
            parentRequestIdStack = Arrays.copyOfRange(parentRequestIdStack, 1, parentRequestIdStack.length);
            parentRequestIdStackStr = StringUtils.join(parentRequestIdStack, OrchestratorConstants.PARENT_REQUEST_ID_DELIM);
        }
        exchange.getIn().setHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, parentRequestIdStackStr);
    }

    public static void createChild(Exchange exchange) throws NoRequestIdPresentException {
        // TODO: Move this to ID generator
        String childId = UUID.randomUUID().toString();

        String requestId = getRequestId(exchange);
        if (StringUtils.isEmpty(requestId)) {
            throw new NoRequestIdPresentException("Request ID absent!");
        }
        String parentIdStack = exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, String.class);
        if (StringUtils.isEmpty(parentIdStack)) {
            parentIdStack = requestId;
        } else {
            parentIdStack = requestId + OrchestratorConstants.PARENT_REQUEST_ID_DELIM + parentIdStack;
        }
        exchange.getIn().setHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, parentIdStack);
        exchange.getIn().setHeader(OrchestratorConstants.REQUEST_ID_HEADER, childId);
    }

    // TODO: Ensure that client sets uniq route ID, we cannot rely on camel generated route ID
    public static String getRouteId(Exchange exchange) {
        String routeId = ROUTE_ID_NOT_DEFINED;
        try {
            routeId = exchange.getUnitOfWork().getRouteContext().getRoute().getId();
        } catch (NullPointerException e) {
            // Ignore
        }
        return routeId;
    }
}
