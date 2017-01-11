package org.apache.camel.cms.orchestrator;

import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.main.Main;

import java.util.List;
import java.util.Map;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class ForkRouteTest extends TestCase {

    private static String rid0 = "rid0";
    private static String rid1 = "rid1";
    private static String rid2 = null;
    private static String rid3 = null;
    private static int forkCount = 0;

    public void testMain() throws Exception {

        Main main = new Main();
        main.addRouteBuilder(new MyRouteBuilder());
        main.enableTrace();
        main.start();

        List<CamelContext> contextList = main.getCamelContexts();
        CamelContext camelContext = contextList.get(0);

        MockEndpoint endpoint = camelContext.getEndpoint("mock:results", MockEndpoint.class);
        endpoint.expectedMinimumMessageCount(1);

        Map<String, Object> headers = Maps.newHashMap();
        headers.put(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, rid0);
        headers.put(OrchestratorConstants.REQUEST_ID_HEADER, rid1);
        main.getCamelTemplate().sendBodyAndHeaders("direct:start", "<message>1</message>", headers);

        endpoint.assertIsSatisfied();
        assertEquals(2, forkCount);

        main.stop();
    }

    public static class MyRouteBuilder extends RouteBuilder {

        @Override
        public void configure() throws Exception {
            from("direct:start")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            assertEquals(rid0, PlatformUtils.getParentRequestId(exchange));
                            assertEquals(rid1, PlatformUtils.getRequestId(exchange));
                        }
                    })
                    .fork("direct:childProcess")
                    .fork("direct:childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            assertEquals(rid0, PlatformUtils.getParentRequestId(exchange));
                            assertEquals(rid1, PlatformUtils.getRequestId(exchange));
                        }
                    })
                    .to("mock:results");

            from("direct:childProcess")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            ++forkCount;
                            rid2 = PlatformUtils.getRequestId(exchange);
                            assertNotNull(rid2);
                            assertEquals(rid1, PlatformUtils.getParentRequestId(exchange));
                        }
                    });

            from("direct:childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            ++forkCount;
                            rid3 = PlatformUtils.getRequestId(exchange);
                            assertNotNull(rid3);
                            assertEquals(rid1, PlatformUtils.getParentRequestId(exchange));
                        }
                    });
        }
    }
}