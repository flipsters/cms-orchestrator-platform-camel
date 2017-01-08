package org.apache.camel.cms.orchestrator.definition;

import org.apache.camel.Endpoint;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.processor.ForkProcessor;
import org.apache.camel.model.SendDefinition;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.RouteContext;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by pawas.kumar on 03/01/17.
 */
@Metadata(label = "eip,endpoint,routing")
@XmlRootElement(name = "fork")
@XmlAccessorType(XmlAccessType.FIELD)
public class ForkDefinition extends SendDefinition<ForkDefinition> {
  @XmlAttribute
  protected ExchangePattern pattern;

  public ForkDefinition() {
  }

  public ForkDefinition(String uri) {
    setUri(uri);
  }

  public ForkDefinition(Endpoint endpoint) {
    setEndpoint(endpoint);
  }

  public ForkDefinition(String uri, ExchangePattern pattern) {
    this(uri);
    this.pattern = pattern;
  }

  public ForkDefinition(Endpoint endpoint, ExchangePattern pattern) {
    this(endpoint);
    this.pattern = pattern;
  }

  @Override
  public String toString() {
    return "Fork[" + getLabel() + "]";
  }

  @Override
  public ExchangePattern getPattern() {
    return pattern;
  }

  /**
   * Sets the optional {@link ExchangePattern} used to invoke this endpoint
   */
  public void setPattern(ExchangePattern pattern) {
    this.pattern = pattern;
  }

  /**
   * Sets the optional {@link ExchangePattern} used to invoke this endpoint
   *
   * @deprecated will be removed in the near future. Instead use {@link org.apache.camel.model.ProcessorDefinition#inOnly()}
   * or {@link org.apache.camel.model.ProcessorDefinition#inOut()}
   */
  @Deprecated
  public ForkDefinition pattern(ExchangePattern pattern) {
    setPattern(pattern);
    return this;
  }

  @Override
  public Processor createProcessor(RouteContext routeContext) throws Exception {
    Endpoint endpoint = resolveEndpoint(routeContext);
    return new ForkProcessor(endpoint, getPattern());
  }

}
