package org.apache.camel.cms.orchestrator.utils;

import org.apache.camel.Exchange;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Stack;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class PlatformUtils {

    public static String getRequestId(Exchange exchange) {
        return ForkUtils.getRequestId(exchange);
    }

    public static String getParentRequestId(Exchange exchange) {
        return ForkUtils.getParentRequestId(exchange);
    }
}
