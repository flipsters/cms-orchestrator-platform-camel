package org.apache.camel.cms.orchestrator.definition;

import org.apache.camel.Endpoint;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Expression;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.factory.JoinCallbackFactory;
import org.apache.camel.cms.orchestrator.processor.JoinProcessor;
import org.apache.camel.cms.orchestrator.processor.WaitForChildrenProcessor;
import org.apache.camel.model.SendDefinition;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.RouteContext;
import org.apache.camel.util.ObjectHelper;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Created by kartik.bommepally on 10/01/17.
 */
@Metadata(label = "eip,endpoint,routing")
@XmlRootElement(name = "waitForChild")
@XmlAccessorType(XmlAccessType.FIELD)
public class WaitForChildrenDefinition extends SendDefinition<WaitForChildrenDefinition> {

    @XmlAttribute(required = true)
    private String aggregatorId;

    @XmlAttribute(required = true)
    private String endpoint;

    @XmlAttribute
    protected ExchangePattern pattern;

    public WaitForChildrenDefinition(String aggregatorId, String endpoint) {
        setUri(JoinCallbackFactory.getCallbackEndpoint());
        this.aggregatorId = aggregatorId;
        this.endpoint = endpoint;
    }

    public WaitForChildrenDefinition(String aggregatorId, String endpoint, ExchangePattern pattern) {
        setUri(JoinCallbackFactory.getCallbackEndpoint());
        this.aggregatorId = aggregatorId;
        this.endpoint = endpoint;
        this.pattern = pattern;
    }

    @Override
    public String toString() {
        return "WaitForChildren[" + aggregatorId + ", " + endpoint + ", " + getLabel() + "]";
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
    public WaitForChildrenDefinition pattern(ExchangePattern pattern) {
        setPattern(pattern);
        return this;
    }

    @Override
    public Processor createProcessor(RouteContext routeContext) throws Exception {
        Endpoint destination = resolveEndpoint(routeContext);
        ObjectHelper.notEmpty(aggregatorId, "aggregatorId", this);
        ObjectHelper.notEmpty(endpoint, "endpoint", this);
        // use simple language for the message string to give it more power
        Expression aggregatorIdExpression = routeContext.getCamelContext().resolveLanguage("simple").createExpression(aggregatorId);
        Expression endpointExpression = routeContext.getCamelContext().resolveLanguage("simple").createExpression(endpoint);
        return new WaitForChildrenProcessor(aggregatorIdExpression, endpointExpression, destination, getPattern());
    }
}
