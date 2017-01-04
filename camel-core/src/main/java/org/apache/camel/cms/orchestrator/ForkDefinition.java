package org.apache.camel.cms.orchestrator;

import lombok.ToString;
import org.apache.camel.model.SendDefinition;

/**
 * Created by pawas.kumar on 03/01/17.
 */
@ToString
public class ForkDefinition extends SendDefinition<ForkDefinition> {

  String uri;

  public ForkDefinition(String uri) {
    setUri(uri);
  }

}
