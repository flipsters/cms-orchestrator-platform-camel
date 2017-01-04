package org.apache.camel.cms.orchestrator;

import flipkart.cms.aggregator.client.AggregateStore;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.processor.DelegateAsyncProcessor;
import org.apache.camel.spi.InterceptStrategy;

/**
 * Created by pawas.kumar on 03/01/17.
 */
public class ForkInterceptor implements InterceptStrategy {

  AggregateStore store;

  @Override
  public Processor wrapProcessorInInterceptors(CamelContext context, ProcessorDefinition<?> definition,
                                               final Processor target, Processor nextTarget) throws Exception {
    if (definition instanceof ForkDefinition) {
      return new DelegateAsyncProcessor(new Processor() {
        @Override
        public void process(Exchange exchange) throws Exception {
          if (exchange.getProperty(OrchestratorConstants.FORK_PROPERTY_NAME) == null) {
            String routeId = exchange.getFromRouteId();
            String requestId = exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER, String.class);// get parent id
            String parentId = exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, String.class);
            store.clear(parentId, routeId);
            store.fork(parentId, requestId, routeId);
          }
          target.process(exchange);
        }
      });
    }
    return target;
  }
}
