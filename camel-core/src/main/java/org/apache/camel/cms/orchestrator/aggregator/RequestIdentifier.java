package org.apache.camel.cms.orchestrator.aggregator;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by pawas.kumar on 01/03/17.
 */
@AllArgsConstructor
@Data
public class RequestIdentifier {
    String requestId;
    String tenantId;

    private static final RequestIdentifier NO_VISIBILITY_REQ_ID = new RequestIdentifier(null, null);

    public static RequestIdentifier noVisibilityReqId() {
        return NO_VISIBILITY_REQ_ID;
    }
}
