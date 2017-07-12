package org.apache.camel.cms.orchestrator.definition;

import com.google.common.collect.Lists;
import org.apache.camel.Expression;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.aggregator.HeterogeneousPayloadAggregator;
import org.apache.camel.cms.orchestrator.factory.JoinCallbackFactory;
import org.apache.camel.cms.orchestrator.processor.WaitForChildrenProcessor;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.ProcessorDefinitionHelper;
import org.apache.camel.model.RecipientListDefinition;
import org.apache.camel.processor.Pipeline;
import org.apache.camel.processor.RecipientList;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.RouteContext;
import org.apache.camel.util.ObjectHelper;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.camel.builder.SimpleBuilder.simple;

/**
 * Created by kartik.bommepally on 10/01/17.
 */
@Metadata(label = "eip,endpoint,routing")
@XmlRootElement(name = "waitForChild")
@XmlAccessorType(XmlAccessType.FIELD)
public class WaitForChildrenDefinition<Type extends ProcessorDefinition<Type>> extends RecipientListDefinition<Type> {

    @XmlAttribute(required = true)
    private HeterogeneousPayloadAggregator payloadAggregator;

    @XmlAttribute(required = true)
    private Expression callbackEndpointExpression;

    public WaitForChildrenDefinition(HeterogeneousPayloadAggregator payloadAggregator, Expression callbackEndpointExpression) {
        super(simple(JoinCallbackFactory.getCallbackEndpoint()));
        this.payloadAggregator = payloadAggregator;
        this.callbackEndpointExpression = callbackEndpointExpression;
    }

    @Override
    public String toString() {
        return getLabel();
    }

    @Override
    public String getLabel() {
        return "Join[" + payloadAggregator.getId() + ", " + callbackEndpointExpression + ", " + super.getLabel() + "]";
    }

    @Override
    public Processor createProcessor(RouteContext routeContext) throws Exception {
        ObjectHelper.notNull(payloadAggregator, "payloadAggregator", this);
        ObjectHelper.notNull(callbackEndpointExpression, "callbackEndpoint", this);
        Pipeline pipeline = (Pipeline) super.createProcessor(routeContext);
        List<Processor> processors = Lists.newArrayList(pipeline.getProcessors());
        RecipientList recipientList = (RecipientList) processors.get(1);
        Expression expression = getExpression().createExpression(routeContext);
        boolean isParallelProcessing = recipientList.isParallelProcessing();
        boolean shutdownThreadPool = ProcessorDefinitionHelper.willCreateNewThreadPool(routeContext,this, isParallelProcessing);
        ExecutorService threadPool = ProcessorDefinitionHelper.getConfiguredExecutorService(routeContext,
                "WaitForChildren", this, isParallelProcessing);
        WaitForChildrenProcessor waitForChildrenProcessor = null;
        String delimiter = getDelimiter();
        if (delimiter == null) {
            waitForChildrenProcessor = new WaitForChildrenProcessor(routeContext.getCamelContext(), expression, payloadAggregator,
                    callbackEndpointExpression, threadPool, shutdownThreadPool, recipientList);
        } else {
            waitForChildrenProcessor = new WaitForChildrenProcessor(routeContext.getCamelContext(), expression, delimiter,
                payloadAggregator, callbackEndpointExpression, threadPool, shutdownThreadPool, recipientList);
        }
        processors.set(1, waitForChildrenProcessor);
        return Pipeline.newInstance(pipeline.getCamelContext(), processors);
    }
}
