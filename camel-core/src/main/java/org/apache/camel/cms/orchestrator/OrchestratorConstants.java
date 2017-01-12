package org.apache.camel.cms.orchestrator;

/**
 * Created by pawas.kumar on 03/01/17.
 */
// TODO: Perhaps, we can have this in the gateway-model jar, which is already being used in PipelineDeployer / GatewayCore / StoresService Jar
public class OrchestratorConstants {
    public static final String IS_FIRST_FORK_PROPERTY = "X-IsFirstFork";
    public static final String REQUEST_ID_HEADER = "X-RequestId";
    public static final String PARENT_REQUEST_ID_HEADER = "X-ParentRequestId";
    public static final String PARENT_REQUEST_ID_DELIM = "<~>";
}
