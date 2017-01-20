package org.apache.camel.cms.orchestrator.utils;

import com.google.common.collect.Lists;
import org.apache.camel.cms.orchestrator.OrchestratorConstants;

import java.util.List;
import java.util.Map;

/**
 * Created by pawas.kumar on 20/01/17.
 */
public class OrchestratorUtils {

  public static List<String> getHeaderTitles() {
    List<String> headers = Lists.newArrayList();
    headers.add(OrchestratorConstants.REQUEST_ID_HEADER);
    headers.add(OrchestratorConstants.PARENT_REQUEST_ID_HEADER);
    headers.add(OrchestratorConstants.TRACK_ID_HEADER);
    headers.add(OrchestratorConstants.TENANT_ID_HEADER);
    headers.add(OrchestratorConstants.PIPELINE_HANDLE_HEADER);
    return headers;
  }

  public static Map<String, Object> removeCoreHeaders(Map<String, Object> headers) {
    for (String title : getHeaderTitles()) {
      headers.remove(title);
    }
    return headers;
  }
}
