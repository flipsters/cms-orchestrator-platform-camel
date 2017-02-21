package org.apache.camel.cms.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.cms.orchestrator.factory.AggregateStoreFactory;
import org.apache.camel.cms.orchestrator.factory.JoinCallbackFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.main.Main;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.camel.support.TypeConverterSupport;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class JoinableForkJoinRouteTest extends TestCase {

    private static InMemoryAggregateStore aggregateStore;
    private static String rid0 = "rid0";
    private static String rid1 = "rid1";
    private static String rid2 = null;
    private static String rid3 = null;
    private static String rid4 = null;
    private static String rid00 = "rid00";
    private static String rid11 = "rid11";
    private static String rid22 = null;
    private static String rid33 = null;
    private static int joinCallbackCount = 0;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void testMain() throws Exception {

        aggregateStore = new InMemoryAggregateStore();
        AggregateStoreFactory.registerStore(aggregateStore);
        JoinCallbackFactory.registerCallbackEndpoint("direct:joinCallback");

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
        headers.put(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, rid0);
        headers.put(OrchestratorConstants.REQUEST_ID_HEADER, rid1);
        main.getCamelTemplate().sendBodyAndHeaders("direct:start", "<message>1</message>", headers);

        endpoint.assertIsSatisfied();
        assertEquals(2, joinCallbackCount);

        headers = Maps.newHashMap();
        headers.put(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, rid00);
        headers.put(OrchestratorConstants.REQUEST_ID_HEADER, rid11);
        main.getCamelTemplate().sendBodyAndHeaders("direct:2start", "<message>1</message>", headers);
        String rid22_ = rid22;
        String rid33_ = rid33;
        assertNotNull(aggregateStore.getRouteMap().get(rid11 + rid22_));
        assertNotNull(aggregateStore.getRouteMap().get(rid11 + rid33_));
        main.getCamelTemplate().sendBodyAndHeaders("direct:2start", "<message>1</message>", headers);
        assertNull(aggregateStore.getRouteMap().get(rid11 + rid22_));
        assertNull(aggregateStore.getRouteMap().get(rid11 + rid33_));
        assertNotNull(aggregateStore.getRouteMap().get(rid11 + rid22));
        assertNotNull(aggregateStore.getRouteMap().get(rid11 + rid33));

        main.stop();
    }

    public void test () throws Exception {
        aggregateStore = new InMemoryAggregateStore();
//        AggregateStoreFactory.registerStore(aggregateStore);
        JoinCallbackFactory.registerCallbackEndpoint("direct:joinCallback");

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
        headers.put(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, rid0);
        headers.put(OrchestratorConstants.REQUEST_ID_HEADER, rid1);
        main.getCamelTemplate().sendBodyAndHeaders("direct:start", Lists.newArrayList(1,2,3,4), headers);
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
                            exchange.getIn().setHeader("XYZ-0", 0);
                        }
                    })
                    .joinableFork("direct:childProcess")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            assertEquals(rid0, PlatformUtils.getParentRequestId(exchange));
                            assertEquals(rid1, PlatformUtils.getRequestId(exchange));
                        }
                    })
                    .joinableFork("direct:childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            assertEquals(rid0, PlatformUtils.getParentRequestId(exchange));
                            assertEquals(rid1, PlatformUtils.getRequestId(exchange));
                        }
                    })
                    .waitForChildren("uvwAggregatorId", "uvwEndpoint")
                    .to("mock:results");

            from("direct:childProcess")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            rid2 = PlatformUtils.getRequestId(exchange);
                            assertNotNull(rid2);
                            assertEquals(rid1, PlatformUtils.getParentRequestId(exchange));
                            assertEquals(0, exchange.getIn().getHeader("XYZ-0"));
                            exchange.setProperty("Hello", "World");
                        }
                    })
                    .joinableFork("direct:childProcess3")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            assertEquals(rid1, PlatformUtils.getParentRequestId(exchange));
                            assertEquals(rid2, PlatformUtils.getRequestId(exchange));
                        }
                    })
                    .waitForChildren("abcAggregatorId", "abcEndpoint")
                    .join("abcAggregatorId");

            from("direct:childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            rid3 = PlatformUtils.getRequestId(exchange);
                            assertNotNull(rid3);
                            assertEquals(rid1, PlatformUtils.getParentRequestId(exchange));
                            assertEquals(0, exchange.getIn().getHeader("XYZ-0"));
                        }
                    })
                    .join("xyzAggregatorId");

            from("direct:childProcess3")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            rid4 = PlatformUtils.getRequestId(exchange);
                            assertNotNull(rid4);
                            assertEquals(rid2, PlatformUtils.getParentRequestId(exchange));
                        }
                    })
                    .join("xyzAggregatorId");

            from("direct:joinCallback")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            ++joinCallbackCount;
                            String parentRequestId = new String(exchange.getIn().getBody(byte[].class));
                            if (parentRequestId.equals(rid1)) {
                                List<String> actualChildren = aggregateStore.getChildMap().get(rid1);
                                List<String> expectedChildren = Lists.newArrayList(rid2, rid3);
                                assertTrue(actualChildren.containsAll(expectedChildren) && expectedChildren.containsAll(actualChildren));
                            } else if (parentRequestId.equals(rid2)) {
                                List<String> actualChildren = aggregateStore.getChildMap().get(rid2);
                                List<String> expectedChildren = Lists.newArrayList(rid4);
                                assertTrue(actualChildren.containsAll(expectedChildren) && expectedChildren.containsAll(actualChildren));
                            } else {
                                throw new AssertionFailedError("Invalid parent request Id " + parentRequestId);
                            }
                        }
                    });

            from("direct:2start")
                    .joinableFork("direct:2childProcess1")
                    .setHeader("X-Next", simple("direct:2childProcess2"))
                    .joinableFork(header("X-Next"))
                    .to("mock:results");

            from("direct:2childProcess1")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            rid22 = PlatformUtils.getRequestId(exchange);
                            assertNotNull(rid22);
                            assertEquals(rid11, PlatformUtils.getParentRequestId(exchange));
                        }
                    })
                    .join("xyzAggregatorId");

            from("direct:2childProcess2")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            rid33 = PlatformUtils.getRequestId(exchange);
                            assertNotNull(rid33);
                            assertEquals(rid11, PlatformUtils.getParentRequestId(exchange));
                        }
                    })
                    .join("xyzAggregatorId");
        }
    }
}