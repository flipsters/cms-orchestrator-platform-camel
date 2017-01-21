package org.apache.camel.cms.orchestrator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import flipkart.cms.aggregator.client.AggregateStore;
import flipkart.cms.aggregator.client.Aggregator;
import flipkart.cms.aggregator.lock.exception.SynchronisedOperationException;
import lombok.Getter;
import org.apache.camel.cms.orchestrator.aggregator.CamelPayloadAggregator;
import org.apache.camel.cms.orchestrator.aggregator.Payload;
import org.apache.camel.cms.orchestrator.utils.OrchestratorUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by kartik.bommepally on 11/01/17.
 */
@Getter
public class InMemoryAggregateStore implements AggregateStore {

    private Map<String, List<String>> childMap = Maps.newHashMap();
    private Map<String, String> routeMap = Maps.newHashMap();
    private Map<String, byte[]> payloadMap = Maps.newHashMap();

    private synchronized void put(String parentId, String childId) {
        if (!childMap.containsKey(parentId)) {
            childMap.put(parentId, Lists.<String>newArrayList());
        }
        childMap.get(parentId).add(childId);
    }

    @Override
    public void fork(String parentId, String childId, String forkRoute) throws IOException {
        put(parentId, childId);
        routeMap.put(parentId + childId, forkRoute);
    }

    private boolean isJoinable(String parentId) {
        for (String childIter : childMap.get(parentId)) {
            if (!payloadMap.containsKey(parentId + childIter)) {
                return false;
            }
        }
        return payloadMap.containsKey(parentId);
    }

    @Override
    public boolean join(String parentId, String childId, byte[] payload, String aggregatorId) throws IOException, SynchronisedOperationException {
        Payload payload1 = null;
        try {
            payload1 = CamelPayloadAggregator.getPayload(payload);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        for (String title : OrchestratorUtils.getCoreHeaderTitles()) {
            if (payload1.getHeaders().get(title) != null) {
                throw new RuntimeException("Header should have been null " + title);
            }
        }
        put(parentId, childId);
        payloadMap.put(parentId + childId, payload);
        return isJoinable(parentId);
    }

    @Override
    public boolean joinWithWait(String parentId, String endpoint, byte[] payload, String aggregatorId) throws IOException, SynchronisedOperationException {
        payloadMap.put(parentId, payload);
        return isJoinable(parentId);
    }

    @Override
    public byte[] aggregate(String parentId) throws IOException {
        return null;
    }

    @Override
    public void register(Aggregator aggregatorImpl) {
    }

    @Override
    public void clear(String parentId, String forkRoute) throws IOException {
        List<String> childIds = childMap.get(parentId);
        if (childIds == null || childIds.isEmpty()) {
            return;
        }
        for (String childId : childIds) {
            if (forkRoute.equals(routeMap.get(parentId + childId))) {
                routeMap.remove(parentId + childId);
                payloadMap.remove(parentId + childId);
            }
        }
    }

    @Override
    public void clear(String parentId) throws IOException {
    }

    @Override
    public String getEndpoint(String parentId) throws IOException {
        return null;
    }
}
