package org.apache.camel.cms.orchestrator.processor;

import flipkart.cms.orchestrator.status.store.api.StatusStoreService;
import flipkart.cms.orchestrator.status.store.model.Status;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.exception.StatusStoreException;
import org.apache.camel.cms.orchestrator.factory.StatusStoreFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.AsyncProcessorHelper;
import org.apache.commons.lang.StringUtils;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class StatusProcessor extends ServiceSupport implements AsyncProcessor, Traceable {

    private final Expression expression;
    private final StatusStoreService statusStoreService;

    public StatusProcessor(Expression expression) {
        this.expression = expression;
        statusStoreService = StatusStoreFactory.getStoreInstance();
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
            Status.StatusBuilder statusBuilder = Status.builder().state(message).requestId(requestId);
            if (StringUtils.isNotEmpty(parentRequestId)) {
                statusBuilder = statusBuilder.parentId(parentRequestId);
            }
            Status status = statusBuilder.build();
            if (!statusStoreService.putStatus(status)) {
                throw new StatusStoreException("Error persisting to status store");
            }
        } catch (Exception e) {
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
