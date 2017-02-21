package org.apache.camel.cms.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.cms.orchestrator.aggregator.TrackIdExtractor;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.factory.AsyncCallbackFactory;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.main.Main;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.camel.support.TypeConverterSupport;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class AsyncTrackRouteTest extends TestCase {

    private static InMemoryAggregateStore aggregateStore;
    private static int asyncCallbackCount = 0;
    private static int externalCall = 0;
    private final ObjectMapper objectMapper = new ObjectMapper();

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
        TypeConverterRegistry typeConverterRegistry = camelContext.getTypeConverterRegistry();
        TypeConverter objectToByte = new TypeConverterSupport() {
            @Override
            public <T> T convertTo(Class<T> type, Object value) throws TypeConversionException {
                try {
                    return (T) objectMapper.writeValueAsBytes(value);
                } catch (IOException e) {
                    throw new TypeConversionException(value, type, e);
                }
            }

            @Override
            public <T> T convertTo(Class<T> type, Exchange exchange, Object value) throws TypeConversionException {
                try {
                    return (T) objectMapper.writeValueAsBytes(value);
                } catch (IOException e) {
                    throw new TypeConversionException(value, type, e.getCause());
                }
            }
        };
        typeConverterRegistry.addTypeConverter(byte[].class, Payload.class, objectToByte);

        MockEndpoint endpoint = camelContext.getEndpoint("mock:results", MockEndpoint.class);
        endpoint.expectedMinimumMessageCount(1);

        Map<String, Object> headers = Maps.newHashMap();
        headers.put("aggregatorId", "aggregatorId");
        headers.put(OrchestratorConstants.ASYNC_TRACK_ID_HEADER, "temp");
        main.getCamelTemplate().sendBodyAndHeaders("direct:start", "<message>1</message>", headers);

        endpoint.assertIsSatisfied();
        assertEquals(1, externalCall);

        headers = Maps.newHashMap();
        headers.put("aggregatorId", "HACK");
        headers.put(OrchestratorConstants.ASYNC_TRACK_ID_HEADER, "temp");
        main.getCamelTemplate().sendBodyAndHeaders("direct:asyncCallback", "<message>1</message>", headers);
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
                            externalCall++;
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