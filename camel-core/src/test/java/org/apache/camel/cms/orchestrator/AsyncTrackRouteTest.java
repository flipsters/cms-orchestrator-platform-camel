package org.apache.camel.cms.orchestrator;

import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.cms.orchestrator.aggregator.TrackIdExtractor;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.factory.AsyncCallbackFactory;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.main.Main;

import java.util.List;
import java.util.Map;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class AsyncTrackRouteTest extends TestCase {

    private static InMemoryAggregateStore aggregateStore;
    private static int asyncCallbackCount = 0;

    public void testMain() throws Exception {

        aggregateStore = new InMemoryAggregateStore();
        AggregateStoreFactory.registerStore(aggregateStore);
        AsyncCallbackFactory.registerCallbackEndpoint("direct:asyncCallback");

        Main main = new Main();
        main.addRouteBuilder(new MyRouteBuilder());
        main.enableTrace();
        main.start();

        List<CamelContext> contextList = main.getCamelContexts();
        CamelContext camelContext = contextList.get(0);

        MockEndpoint endpoint = camelContext.getEndpoint("mock:results", MockEndpoint.class);
        endpoint.expectedMinimumMessageCount(1);

        Map<String, Object> headers = Maps.newHashMap();
        headers.put("aggregatorId", "aggregatorId");
        main.getCamelTemplate().sendBodyAndHeaders("direct:start", "<message>1</message>", headers);

        endpoint.assertIsSatisfied();
        assertEquals(0, asyncCallbackCount);

        headers = Maps.newHashMap();
        headers.put("aggregatorId", "HACK");
        main.getCamelTemplate().sendBodyAndHeaders("direct:start", "<message>1</message>", headers);
        assertEquals(1, asyncCallbackCount);

        main.stop();
    }

    public static class MyRouteBuilder extends RouteBuilder {

        @Override
        public void configure() throws Exception {
            from("direct:start")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                        }
                    })
                    .asyncTrack(simple("direct:externalEndpoint"), simple("callbackEndpoint"), header("aggregatorId"), new TrackIdExtractor() {
                        @Override
                        public String getTrackId(Exchange exchange) {
                            return exchange.getIn().getHeader("tenantId", String.class);
                        }
                    })
                    .to("mock:results");

            from("direct:externalEndpoint")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            exchange.getIn().setHeader("tenantId", "tenantId");
                        }
                    });

            from("direct:asyncCallback")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            ++asyncCallbackCount;
                        }
                    });
        }
    }
}