package org.apache.camel.cms.orchestrator.utils;

import org.apache.camel.Exchange;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.commons.collections.CollectionUtils;

import java.util.Stack;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class PlatformUtils {

    public static String getRequestId(Exchange exchange) {
        return exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER, String.class);
    }

    public static String getParentRequestId(Exchange exchange) {
        Stack<String> stack = exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER, Stack.class);
        if(CollectionUtils.isEmpty(stack)) {
            return null;
        }

        return stack.peek();
    }
}
