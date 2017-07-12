package org.apache.camel.cms.orchestrator.definition;

import com.google.common.collect.Lists;
import org.apache.camel.Expression;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.aggregator.AsyncAckExtractor;
import org.apache.camel.cms.orchestrator.aggregator.AsyncPayloadTransformer;
import org.apache.camel.cms.orchestrator.aggregator.HeterogeneousPayloadAggregator;
import org.apache.camel.cms.orchestrator.processor.AsyncTrackProcessor;
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
@XmlRootElement(name = "asyncTrack")
@XmlAccessorType(XmlAccessType.FIELD)
public class AsyncTrackDefinition<Type extends ProcessorDefinition<Type>> extends RecipientListDefinition<Type> {


    @XmlAttribute(required = true)
    private Expression callbackEndpointExpression;

    @XmlAttribute(required = true)
    private HeterogeneousPayloadAggregator payloadAggregator;

    @XmlAttribute(required = true)
    private AsyncAckExtractor asyncAckExtractor;

    @XmlAttribute(required = true)
    private AsyncPayloadTransformer asyncPayloadTransformer;

    @XmlAttribute(required = true)
    private RecipientListDefinition<Type> asyncCallbackDefinition;

    @XmlAttribute(required = true)
    private Long expiryBreachTime;

    public AsyncTrackDefinition(Expression externalEndpoint, Expression callbackEndpointExpression,
                                HeterogeneousPayloadAggregator payloadAggregator, AsyncPayloadTransformer asyncPayloadTransformer, AsyncAckExtractor asyncAckExtractor,
                                RecipientListDefinition<Type> asyncCallbackDefinition, Long expiryBreachTime) {
        super(externalEndpoint);
        this.callbackEndpointExpression = callbackEndpointExpression;
        this.payloadAggregator = payloadAggregator;
        this.asyncPayloadTransformer = asyncPayloadTransformer;
        this.asyncAckExtractor = asyncAckExtractor;
        this.asyncCallbackDefinition = asyncCallbackDefinition;
        this.expiryBreachTime = expiryBreachTime;
    }

    public AsyncTrackDefinition(Expression externalEndpoint, Expression callbackEndpointExpression,
                                HeterogeneousPayloadAggregator payloadAggregator, AsyncPayloadTransformer asyncPayloadTransformer, AsyncAckExtractor asyncAckExtractor,
                                RecipientListDefinition<Type> asyncCallbackDefinition) {
        this(externalEndpoint, callbackEndpointExpression, payloadAggregator, asyncPayloadTransformer, asyncAckExtractor, asyncCallbackDefinition, null);
    }

    @Override
    public String toString() {
        return getLabel();
    }

    @Override
    public String getLabel() {
        return "AsyncTrack[" + callbackEndpointExpression + ", " + payloadAggregator.getId() + ", " + super.getLabel() + "]";
    }

    @Override
    public Processor createProcessor(RouteContext routeContext) throws Exception {
        ObjectHelper.notNull(callbackEndpointExpression, "callbackEndpoint", this);
        ObjectHelper.notNull(payloadAggregator, "payloadAggregator", this);
        ObjectHelper.notNull(asyncAckExtractor, "asyncAckExtractor", this);
        ObjectHelper.notNull(asyncPayloadTransformer, "callbackUrlAppender", this);
        Pipeline pipeline = (Pipeline) super.createProcessor(routeContext);
        List<Processor> processors = Lists.newArrayList(pipeline.getProcessors());
        RecipientList recipientList = (RecipientList) processors.get(1);
        Expression expression = getExpression().createExpression(routeContext);
        boolean isParallelProcessing = recipientList.isParallelProcessing();
        boolean shutdownThreadPool = ProcessorDefinitionHelper.willCreateNewThreadPool(routeContext,this, isParallelProcessing);
        ExecutorService threadPool = ProcessorDefinitionHelper.getConfiguredExecutorService(routeContext,
                "Join", this, isParallelProcessing);
        AsyncTrackProcessor asyncTrackProcessor = null;
        String delimiter = getDelimiter();
        Pipeline asyncCallbackPipeline = (Pipeline) asyncCallbackDefinition.createProcessor(routeContext);
        List<Processor> asyncCallbackProcessors = Lists.newArrayList(asyncCallbackPipeline.getProcessors());
        RecipientList asyncCallbackRecipientList = (RecipientList) asyncCallbackProcessors.get(1);
        if (delimiter == null) {
            asyncTrackProcessor = new AsyncTrackProcessor(routeContext.getCamelContext(), expression, callbackEndpointExpression,
                payloadAggregator, asyncPayloadTransformer, asyncAckExtractor, asyncCallbackRecipientList, threadPool, shutdownThreadPool, recipientList, expiryBreachTime);
        } else {
            asyncTrackProcessor = new AsyncTrackProcessor(routeContext.getCamelContext(), expression, delimiter,
                    callbackEndpointExpression, payloadAggregator, asyncPayloadTransformer, asyncAckExtractor, asyncCallbackRecipientList, threadPool,
                    shutdownThreadPool, recipientList, expiryBreachTime);
        }
        processors.set(1, asyncTrackProcessor);
        return Pipeline.newInstance(pipeline.getCamelContext(), processors);
    }
}
