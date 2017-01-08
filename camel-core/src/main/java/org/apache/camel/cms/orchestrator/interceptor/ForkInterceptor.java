package org.apache.camel.cms.orchestrator.interceptor;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;
import org.apache.camel.cms.orchestrator.definition.ForkDefinition;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.processor.DelegateAsyncProcessor;
import org.apache.camel.spi.InterceptStrategy;

/**
 * Created by pawas.kumar on 03/01/17.
 */
public class ForkInterceptor implements InterceptStrategy {

  private AggregateStore aggregateStore;

  public ForkInterceptor() {
    aggregateStore = AggregateStoreFactory.getStoreInstance();
  }

  @Override
  public Processor wrapProcessorInInterceptors(CamelContext context, ProcessorDefinition<?> definition,
                                               final Processor target, Processor nextTarget) throws Exception {
    if (definition instanceof ForkDefinition) {
      return new DelegateAsyncProcessor(new Processor() {
        @Override
        public void process(Exchange exchange) throws Exception {
          if (exchange.getProperty(OrchestratorConstants.FORK_PROPERTY_NAME) == null) {
            String routeId = exchange.getFromRouteId();
            String requestId = PlatformUtils.getRequestId(exchange);
            String parentId = PlatformUtils.getParentRequestId(exchange);
            aggregateStore.clear(parentId, routeId);
            aggregateStore.fork(parentId, requestId, routeId);
          }
          target.process(exchange);
        }
      });
    }
    return target;
  }
}
