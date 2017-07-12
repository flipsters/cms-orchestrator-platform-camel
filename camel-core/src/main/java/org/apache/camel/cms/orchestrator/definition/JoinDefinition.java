package org.apache.camel.cms.orchestrator.definition;

import com.google.common.collect.Lists;
import org.apache.camel.Endpoint;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Expression;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.aggregator.PayloadAggregator;
import org.apache.camel.cms.orchestrator.factory.JoinCallbackFactory;
import org.apache.camel.cms.orchestrator.processor.ForkProcessor;
import org.apache.camel.cms.orchestrator.processor.JoinProcessor;
import org.apache.camel.cms.orchestrator.processor.JoinableForkProcessor;
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
 * Created by pawas.kumar on 03/01/17.
 */
@Metadata(label = "eip,endpoint,routing")
@XmlRootElement(name = "join")
@XmlAccessorType(XmlAccessType.FIELD)
public class JoinDefinition<Type extends ProcessorDefinition<Type>> extends RecipientListDefinition<Type> {

    @XmlAttribute(required = true)
    private PayloadAggregator payloadAggregator;

    public JoinDefinition(PayloadAggregator payloadAggregator) {
        super(simple(JoinCallbackFactory.getCallbackEndpoint()));
        this.payloadAggregator = payloadAggregator;
    }

    @Override
    public String toString() {
        return getLabel();
    }

    @Override
    public String getLabel() {
        return "Join[" + payloadAggregator.getId() + ", " + super.getLabel() + "]";
    }

    @Override
    public Processor createProcessor(RouteContext routeContext) throws Exception {
        ObjectHelper.notNull(payloadAggregator, "payloadAggregator", this);
        Pipeline pipeline = (Pipeline) super.createProcessor(routeContext);
        List<Processor> processors = Lists.newArrayList(pipeline.getProcessors());
        RecipientList recipientList = (RecipientList) processors.get(1);
        Expression expression = getExpression().createExpression(routeContext);
        boolean isParallelProcessing = recipientList.isParallelProcessing();
        boolean shutdownThreadPool = ProcessorDefinitionHelper.willCreateNewThreadPool(routeContext,this, isParallelProcessing);
        ExecutorService threadPool = ProcessorDefinitionHelper.getConfiguredExecutorService(routeContext,
                "Join", this, isParallelProcessing);
        JoinProcessor joinProcessor = null;
        String delimiter = getDelimiter();
        if (delimiter == null) {
            joinProcessor = new JoinProcessor(routeContext.getCamelContext(), expression, payloadAggregator, threadPool, shutdownThreadPool, recipientList);
        } else {
            joinProcessor = new JoinProcessor(routeContext.getCamelContext(), expression, delimiter, payloadAggregator, threadPool, shutdownThreadPool, recipientList);
        }
        processors.set(1, joinProcessor);
        return Pipeline.newInstance(pipeline.getCamelContext(), processors);
    }
}
