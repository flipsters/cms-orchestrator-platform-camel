package org.apache.camel.cms.orchestrator;

import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.cms.orchestrator.factory.StatusStoreFactory;
import org.apache.camel.cms.orchestrator.utils.PlatformUtils;
import org.apache.camel.main.Main;

import java.util.Map;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class StatusRouteTest extends TestCase {

    private static String rid0 = "rid0";
    private static String rid1 = "rid1";
    private static String rid2 = null;

//    public void testMain() throws Exception {
//        InMemoryStatusStoreService statusStoreService = new InMemoryStatusStoreService();
//        StatusStoreFactory.registerStore(statusStoreService);
//
//        // lets make a simple route
//        Main main = new Main();
//        main.addRouteBuilder(new MyRouteBuilder());
//        main.enableTrace();
//        main.start();
//
//        Map<String, Object> headers = Maps.newHashMap();
//        headers.put(OrchestratorConstants.PARENT_REQUEST_ID_HEADER, rid0);
//        headers.put(OrchestratorConstants.REQUEST_ID_HEADER, rid1);
//        main.getCamelTemplate().sendBodyAndHeaders("direct:start", "<message>1</message>", headers);
//
//        assertEquals(DetailedStatus.builder().status(
//                Status.builder().state("I_AM_PARENT").requestId(rid1).parentId(rid0).build()).build(),
//                statusStoreService.getStatus(rid1));
//        assertEquals(DetailedStatus.builder().status(
//                Status.builder().state("I_AM_CHILD").requestId(rid2).parentId(rid1).build()).build(),
//                statusStoreService.getStatus(rid2));
//
//        main.stop();
//    }
//
//    public static class MyRouteBuilder extends RouteBuilder {
//        @Override
//        public void configure() throws Exception {
//            from("direct:start")
//                    .status("I_AM_PARENT")
//                    .fork("direct:end");
//            from("direct:end")
//                    .process(new Processor() {
//                        @Override
//                        public void process(Exchange exchange) throws Exception {
//                            rid2 = PlatformUtils.getRequestId(exchange);
//                        }
//                    })
//                    .status("I_AM_CHILD");
//        }
//    }
//
//    private class InMemoryStatusStoreService implements StatusStoreService {
//
//        private Map<String, String> stateMap = Maps.newConcurrentMap();
//        private Map<String, String> parentMap = Maps.newConcurrentMap();
//
//        @Override
//        public DetailedStatus getStatus(String requestId) {
//            return DetailedStatus.builder().status(Status.builder().state(stateMap.get(requestId))
//                    .requestId(requestId).parentId(parentMap.get(requestId)).build()).build();
//        }
//
//        @Override
//        public boolean putStatus(Status status) {
//            stateMap.put(status.getRequestId(), status.getState());
//            parentMap.put(status.getRequestId(), status.getParentId());
//            return true;
//        }
//
//        @Override
//        public NestedStatus getNestedStatus(String requestId) {
//            return null;
//        }
//    }
}
