package org.apache.camel.cms.orchestrator;

import flipkart.cms.orchestrator.status.store.api.StatusStoreService;
import flipkart.cms.orchestrator.status.store.model.DetailedStatus;
import flipkart.cms.orchestrator.status.store.model.NestedStatus;
import flipkart.cms.orchestrator.status.store.model.Status;
import junit.framework.TestCase;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.cms.orchestrator.factory.StatusStoreFactory;
import org.apache.camel.main.Main;

import java.util.List;

/**
 * Created by achit.ojha on 08/01/17.
 */
public class StatusRouteTest extends TestCase {

    public void testMain() throws Exception {
        StatusStoreFactory.registerStore(new MockStatusStoreService());

        // lets make a simple route
        Main main = new Main();
        main.addRouteBuilder(new MyRouteBuilder());
        main.enableTrace();
        main.start();

        main.getCamelTemplate().sendBody("direct:start", "<message>1</message>");

        main.stop();
    }

    public static class MyRouteBuilder extends RouteBuilder {
        @Override
        public void configure() throws Exception {
            from("direct:start")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            exchange.getIn().setHeader(OrchestratorConstants.REQUEST_ID_HEADER, "P1");
                            exchange.getIn().setHeader("H1", "myCustomAuditMessage");
                        }
                    })
                    .status("header.H1");
        }
    }

    private class MockStatusStoreService implements StatusStoreService {

        @Override
        public DetailedStatus getStatus(String s) {
            return null;
        }

        @Override
        public boolean putStatus(Status status) {
            System.out.println(status);
            return true;
        }

        @Override
        public NestedStatus getNestedStatus(String s) {
            return null;
        }
    }
}
