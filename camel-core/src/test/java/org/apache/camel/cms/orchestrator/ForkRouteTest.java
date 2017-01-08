package org.apache.camel.cms.orchestrator;

import junit.framework.TestCase;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.main.Main;

import java.util.List;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class ForkRouteTest extends TestCase {

    public void testMain() throws Exception {
        // lets make a simple route
        Main main = new Main();
        main.addRouteBuilder(new MyRouteBuilder());
        main.enableTrace();
        main.bind("foo", new Integer(31));
        main.start();

        List<CamelContext> contextList = main.getCamelContexts();
        assertNotNull(contextList);
        assertEquals("Did not get the expected count of Camel contexts", 1, contextList.size());
        CamelContext camelContext = contextList.get(0);
        assertEquals("Could not find the registry bound object", 31, camelContext.getRegistry().lookupByName("foo"));

        MockEndpoint endpoint = camelContext.getEndpoint("mock:results", MockEndpoint.class);
        endpoint.expectedMinimumMessageCount(1);

        main.getCamelTemplate().sendBody("direct:start", "<message>1</message>");

        endpoint.assertIsSatisfied();

        main.stop();
    }

    public static class MyRouteBuilder extends RouteBuilder {
        @Override
        public void configure() throws Exception {
            from("direct:start")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            exchange.getIn().setHeader(OrchestratorConstants.REQUEST_ID_HEADER, "rid1");
                            System.out.println("PARENT BEFORE FORK");
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER));
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER));
                        }
                    })
                    .fork("direct:childProcess")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            System.out.println("PARENT AFTER FORK");
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER));
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER));
                        }
                    })
                    .to("mock:results");

            from("direct:childProcess")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            System.out.println("CHILD1 BEFORE FORK");
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER));
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER));
                        }
                    })
                    .fork("direct:childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            System.out.println("CHILD1 AFTER FORK");
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER));
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER));
                        }
                    });

            from("direct:childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            System.out.println("CHILD2 ::");
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.REQUEST_ID_HEADER));
                            System.out.println(exchange.getIn().getHeader(OrchestratorConstants.PARENT_REQUEST_ID_HEADER));
                        }
                    });
        }
    }
}