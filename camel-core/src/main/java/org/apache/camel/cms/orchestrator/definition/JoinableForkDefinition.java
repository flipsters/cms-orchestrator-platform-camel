package org.apache.camel.cms.orchestrator.definition;

import com.google.common.collect.Lists;
import org.apache.camel.Endpoint;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Expression;
import org.apache.camel.Processor;
import org.apache.camel.cms.orchestrator.processor.ForkProcessor;
import org.apache.camel.cms.orchestrator.processor.JoinableForkProcessor;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.ProcessorDefinitionHelper;
import org.apache.camel.processor.Pipeline;
import org.apache.camel.processor.RecipientList;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.RouteContext;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by pawas.kumar on 03/01/17.
 */
@Metadata(label = "eip,endpoint,routing")
@XmlRootElement(name = "joinableFork")
@XmlAccessorType(XmlAccessType.FIELD)
public class JoinableForkDefinition<Type extends ProcessorDefinition<Type>> extends ForkDefinition<Type> {


  public JoinableForkDefinition() {
  }

  public JoinableForkDefinition(Expression recipients) {
    super(recipients);
  }

  @Override
  public String getLabel() {
    return "JoinableFork[" + super.getLabel() + "]";
  }

  @Override
  public Processor createProcessor(RouteContext routeContext) throws Exception {
    Pipeline pipeline = (Pipeline) super.createProcessor(routeContext);
    List<Processor> processors = Lists.newArrayList(pipeline.getProcessors());
    RecipientList recipientList = (RecipientList) processors.get(1);
    Expression expression = getExpression().createExpression(routeContext);
    boolean isParallelProcessing = recipientList.isParallelProcessing();
    boolean shutdownThreadPool = ProcessorDefinitionHelper.willCreateNewThreadPool(routeContext,this, isParallelProcessing);
    ExecutorService threadPool = ProcessorDefinitionHelper.getConfiguredExecutorService(routeContext,
            "JoinableFork", this, isParallelProcessing);
    JoinableForkProcessor joinableForkProcessor = null;
    String delimiter = getDelimiter();
    if (delimiter == null) {
      joinableForkProcessor = new JoinableForkProcessor(routeContext.getCamelContext(), expression, threadPool, shutdownThreadPool, recipientList);
    } else {
      joinableForkProcessor = new JoinableForkProcessor(routeContext.getCamelContext(), expression, delimiter, threadPool, shutdownThreadPool, recipientList);
    }
    processors.set(1, joinableForkProcessor);
    return Pipeline.newInstance(pipeline.getCamelContext(), processors);
  }
}
