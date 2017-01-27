package org.apache.camel.cms.orchestrator.utils;

import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;

import java.util.Map;

/**
 * Created by kartik.bommepally on 27/01/17.
 */
@Data
public class PlatformContext {

    private Map<String, Object> context = Maps.newConcurrentMap();

    public boolean isRoutesFirstFork() {
        return !context.containsKey(OrchestratorConstants.IS_FIRST_FORK_PROPERTY);
    }

    public void markForkDone() {
        context.put(OrchestratorConstants.IS_FIRST_FORK_PROPERTY, new Object());
    }
}
