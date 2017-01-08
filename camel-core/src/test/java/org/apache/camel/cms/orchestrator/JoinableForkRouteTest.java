package org.apache.camel.cms.orchestrator;

import flipkart.cms.aggregator.client.AggregateStore;
import flipkart.cms.aggregator.client.Aggregator;
import flipkart.cms.aggregator.lock.exception.SynchronisedOperationException;
import junit.framework.TestCase;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.main.Main;

import java.io.IOException;
import java.util.List;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class JoinableForkRouteTest extends TestCase {

    public void testMain() throws Exception {
        AggregateStoreFactory.registerStore(new MockAggStoreImpl());

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
                            System.out.println(PlatformUtils.getRequestId(exchange));
                            System.out.println(PlatformUtils.getParentRequestId(exchange));
                        }
                    })
                    .joinableFork("direct:childProcess")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            System.out.println("PARENT AFTER FORK");
                            System.out.println(PlatformUtils.getRequestId(exchange));
                            System.out.println(PlatformUtils.getParentRequestId(exchange));
                        }
                    })
                    .to("mock:results");

            from("direct:childProcess")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            System.out.println("CHILD1 BEFORE FORK");
                            System.out.println(PlatformUtils.getRequestId(exchange));
                            System.out.println(PlatformUtils.getParentRequestId(exchange));
                        }
                    })
                    .joinableFork("direct:childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            System.out.println("CHILD1 AFTER FORK");
                            System.out.println(PlatformUtils.getRequestId(exchange));
                            System.out.println(PlatformUtils.getParentRequestId(exchange));
                        }
                    });

            from("direct:childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            System.out.println("CHILD2 ::");
                            System.out.println(PlatformUtils.getRequestId(exchange));
                            System.out.println(PlatformUtils.getParentRequestId(exchange));
                        }
                    });
        }
    }

    class MockAggStoreImpl implements AggregateStore {

        @Override
        public void fork(String s, String s1, String s2) throws IOException {

        }

        @Override
        public boolean join(String s, String s1, byte[] bytes, String s2) throws IOException, SynchronisedOperationException {
            return true;
        }

        @Override
        public boolean joinWithWait(String s, String s1, byte[] bytes, String s2) throws IOException, SynchronisedOperationException {
            return false;
        }

        @Override
        public byte[] aggregate(String s) throws IOException {
            return new byte[0];
        }

        @Override
        public void register(Aggregator aggregator) {

        }

        @Override
        public void clear(String s, String s1) throws IOException {

        }

        @Override
        public void clear(String s) throws IOException {

        }

        @Override
        public String getEndpoint(String s) throws IOException {
            return null;
        }
    }
}