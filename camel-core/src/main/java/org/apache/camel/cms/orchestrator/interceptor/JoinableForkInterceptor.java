package org.apache.camel.cms.orchestrator.interceptor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.definition.JoinableForkDefinition;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.processor.DelegateAsyncProcessor;
import org.apache.camel.spi.InterceptStrategy;

/**
 * Created by pawas.kumar on 03/01/17.
 */
public class JoinableForkInterceptor implements InterceptStrategy {

    private AggregateStore aggregateStore;

    public JoinableForkInterceptor() {
        aggregateStore = AggregateStoreFactory.getStoreInstance();
    }

    @Override
    public Processor wrapProcessorInInterceptors(CamelContext context, ProcessorDefinition<?> definition,
                                                 final Processor target, Processor nextTarget) throws Exception {
        // TODO: Add WaitForJoinDefinition also
        if (definition instanceof JoinableForkDefinition) {
            return new DelegateAsyncProcessor(new Processor() {
                @Override
                public void process(Exchange exchange) throws Exception {
                    if (exchange.getProperty(OrchestratorConstants.IS_FIRST_FORK_PROPERTY) == null) {
                        String routeId = exchange.getUnitOfWork().getRouteContext().getRoute().getId();
                        String requestId = PlatformUtils.getRequestId(exchange);
                        aggregateStore.clear(requestId, routeId);
                        exchange.setProperty(OrchestratorConstants.IS_FIRST_FORK_PROPERTY, true);
                    }
                    target.process(exchange);
                }
            });
        }
        return target;
    }
}
