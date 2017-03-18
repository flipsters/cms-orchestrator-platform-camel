package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.aggregator.client.StatusStore;
import flipkart.cms.aggregator.model.Status;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.factory.StatusStoreFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.AsyncProcessorHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class StatusProcessor extends ServiceSupport implements AsyncProcessor, Traceable {

    private static final Logger LOG = LoggerFactory.getLogger(StatusProcessor.class);

    private final Expression expression;
    private final StatusStore statusStore;

    public StatusProcessor(Expression expression) {
        this.expression = expression;
        statusStore = StatusStoreFactory.getStoreInstance();
    }

    public void process(Exchange exchange) throws Exception {
        AsyncProcessorHelper.process(this, exchange);
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        try {
            String message = expression.evaluate(exchange, String.class);
            String requestId = PlatformUtils.getRequestId(exchange);
            String parentRequestId = PlatformUtils.getParentRequestId(exchange);
            Integer statusCounter = PlatformUtils.getStatusCounter(exchange);
            String requestPath = PlatformUtils.getRequestPath(exchange);
            Status.StatusBuilder statusBuilder = Status.builder().status(message).requestId(requestId)
                .counter(statusCounter).parentRequestId(parentRequestId).requestPath(requestPath);
            Status status = statusBuilder.build();
            if (!statusStore.putStatus(status)) {
                LOG.info("Not able to update the status due to version mis-match");
            }
            exchange.getIn().setHeader(OrchestratorConstants.STATUS_COUNTER, statusCounter + 1);
        } catch (Exception e) {
            LOG.error("Failed to push status for " + exchange.getIn().getHeaders(), e);
            exchange.setException(e);
        } finally {
            // callback must be invoked
            callback.done(true);
        }
        return true;
    }

    @Override
    public String toString() {
        return "Audit[" + expression + "]";
    }

    public String getTraceLabel() {
        return "Audit[" + expression + "]";
    }

    @Override
    protected void doStart() throws Exception {
        // noop
    }

    @Override
    protected void doStop() throws Exception {
        // noop
    }
}
