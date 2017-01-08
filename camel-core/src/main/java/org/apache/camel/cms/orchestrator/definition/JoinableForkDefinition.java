package org.apache.camel.cms.orchestrator.definition;

import org.apache.camel.Endpoint;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.processor.JoinableForkProcessor;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.RouteContext;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by pawas.kumar on 03/01/17.
 */
@Metadata(label = "eip,endpoint,routing")
@XmlRootElement(name = "fork")
@XmlAccessorType(XmlAccessType.FIELD)
public class JoinableForkDefinition extends ForkDefinition {

  public JoinableForkDefinition(String uri) {
    setUri(uri);
  }

  public JoinableForkDefinition(Endpoint endpoint) {
    setEndpoint(endpoint);
  }

  public JoinableForkDefinition(String uri, ExchangePattern pattern) {
    this(uri);
    this.pattern = pattern;
  }

  public JoinableForkDefinition(Endpoint endpoint, ExchangePattern pattern) {
    this(endpoint);
    this.pattern = pattern;
  }

  @Override
  public String toString() {
    return "JoinableFork[" + getLabel() + "]";
  }

  @Override
  public Processor createProcessor(RouteContext routeContext) throws Exception {
    Endpoint endpoint = resolveEndpoint(routeContext);
    return new JoinableForkProcessor(endpoint, getPattern());
  }

}
