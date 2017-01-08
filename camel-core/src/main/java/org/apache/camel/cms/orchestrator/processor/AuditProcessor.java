package org.apache.camel.cms.orchestrator.processor;

import org.apache.camel.*;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.AsyncProcessorHelper;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class AuditProcessor  extends ServiceSupport implements AsyncProcessor, Traceable {

    private final Expression expression;

    public AuditProcessor(Expression expression) {
        this.expression = expression;
    }

    public void process(Exchange exchange) throws Exception {
        AsyncProcessorHelper.process(this, exchange);
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        try {
            String msg = expression.evaluate(exchange, String.class);
            // TODO :: Push msg via audit interface
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
