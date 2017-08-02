package org.apache.camel.cms.orchestrator.utils;

import org.apache.camel.Exchange;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class PlatformUtils {

    public static String getRequestId(Exchange exchange) {
        return ForkUtils.getRequestId(exchange);
    }

    public static String getRequestId(Payload payload) {
        return ForkUtils.getRequestId(payload);
    }

    public static String getTenantId(Exchange exchange) {
        return exchange.getIn().getHeader(OrchestratorConstants.TENANT_ID_HEADER, String.class);
    }

    public static String getParentRequestId(Exchange exchange) {
        return ForkUtils.getParentRequestId(exchange);
    }

    public static Integer getStatusCounter(Exchange exchange) {
        return exchange.getIn().getHeader(OrchestratorConstants.STATUS_COUNTER, 0, Integer.class);
    }

    public static void addPlatformContext(Map<String, Object> properties) {
        if (!properties.containsKey(OrchestratorConstants.PLATFORM_CONTEXT_PROPERTY)) {
            properties.put(OrchestratorConstants.PLATFORM_CONTEXT_PROPERTY, new PlatformContext());
        }
    }

    public static String requestIdStack(Exchange exchange) {
        String requestId = getRequestId(exchange);
        String parentIdStack = exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, String.class);
        if (StringUtils.isEmpty(parentIdStack)) {
            return requestId;
        }
        return requestId + OrchestratorConstants.PARENT_REQUEST_ID_DELIM + parentIdStack;
    }

    public static String requestIdStack(Payload payload) {
        String requestId = getRequestId(payload);
        String parentIdStack = (String) payload.getHeaders().get(OrchestratorConstants.PARENT_REQUEST_ID_HEADER);
        if (StringUtils.isEmpty(parentIdStack)) {
            return requestId;
        }
        return requestId + OrchestratorConstants.PARENT_REQUEST_ID_DELIM + parentIdStack;
    }

    public static String getRequestPath(Exchange exchange) {
        return exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, String.class);
    }
}
