package org.apache.camel.cms.orchestrator.definition;

import org.apache.camel.Expression;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.processor.AuditProcessor;
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
@XmlRootElement(name = "audit")
@XmlAccessorType(XmlAccessType.FIELD)
public class AuditDefinition extends NoOutputDefinition<AuditDefinition> {

    @XmlTransient
    private static final Logger LOG = LoggerFactory.getLogger(AuditDefinition.class);

    @XmlAttribute(required = true)
    private String message;

    public AuditDefinition() {
    }

    public AuditDefinition(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Audit[" + message + "]";
    }

    @Override
    public String getLabel() {
        return "audit";
    }

    @Override
    public Processor createProcessor(RouteContext routeContext) throws Exception {
        ObjectHelper.notEmpty(message, "message", this);

        // use simple language for the message string to give it more power
        Expression exp = routeContext.getCamelContext().resolveLanguage("simple").createExpression(message);

        return new AuditProcessor(exp);
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
     * Sets the log message (uses simple language)
     */
    public void setMessage(String message) {
        this.message = message;
    }

}
