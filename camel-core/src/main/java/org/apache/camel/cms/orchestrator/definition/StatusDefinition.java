package org.apache.camel.cms.orchestrator.definition;

import org.apache.camel.Expression;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.processor.StatusProcessor;
import org.apache.camel.model.NoOutputDefinition;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.RouteContext;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.*;

/**
 * Created by achit.ojha on 08/01/17.
 */
@Metadata(label = "configuration")
@XmlRootElement(name = "status")
@XmlAccessorType(XmlAccessType.FIELD)
public class StatusDefinition extends NoOutputDefinition<StatusDefinition> {

    @XmlTransient
    private static final Logger LOG = LoggerFactory.getLogger(StatusDefinition.class);

    @XmlAttribute(required = true)
    private String message;

    public StatusDefinition() {
    }

    public StatusDefinition(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Status[" + message + "]";
    }

    @Override
    public String getLabel() {
        return "status";
    }

    @Override
    public Processor createProcessor(RouteContext routeContext) throws Exception {
        ObjectHelper.notEmpty(message, "message", this);

        // use simple language for the message string to give it more power
        Expression exp = routeContext.getCamelContext().resolveLanguage("simple").createExpression(message);

        return new StatusProcessor(exp);
    }

    @Override
    public void addOutput(ProcessorDefinition<?> output) {
        // add outputs on parent as this log does not support outputs
        getParent().addOutput(output);
    }


    public String getMessage() {
        return message;
    }

    /**
     * Sets the status message (uses simple language)
     */
    public void setMessage(String message) {
        this.message = message;
    }

}
