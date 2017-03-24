package org.apache.camel.cms.orchestrator.aggregator;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.camel.Exchange;

/**
 * Created by kartik.bommepally on 23/03/17.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NoVisibilityAsyncAckExtractor implements AsyncAckExtractor {

    @Getter
    private static final NoVisibilityAsyncAckExtractor instance = new NoVisibilityAsyncAckExtractor();

    @Override
    public RequestIdentifier getRequestIdentifier(Exchange exchange) {
        return RequestIdentifier.noVisibilityReqId();
    }
}
