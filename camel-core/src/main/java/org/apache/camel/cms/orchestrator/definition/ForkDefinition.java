package org.apache.camel.cms.orchestrator.definition;

import com.google.common.collect.Lists;
import org.apache.camel.*;
import org.apache.camel.cms.orchestrator.processor.ForkProcessor;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.ProcessorDefinitionHelper;
import org.apache.camel.model.RecipientListDefinition;
import org.apache.camel.model.SendDefinition;
import org.apache.camel.processor.Pipeline;
import org.apache.camel.processor.RecipientList;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.RouteContext;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by pawas.kumar on 03/01/17.
 */
@Metadata(label = "eip,endpoint,routing")
@XmlRootElement(name = "fork")
@XmlAccessorType(XmlAccessType.FIELD)
public class ForkDefinition<Type extends ProcessorDefinition<Type>> extends RecipientListDefinition<Type> {

  public ForkDefinition() {
  }

  public ForkDefinition(Expression recipient) {
    super(recipient);
  }

  @Override
  public String toString() {
    return getLabel();
  }

  @Override
  public String getLabel() {
    return "Fork[" + super.getLabel() + "]";
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
            "Fork", this, isParallelProcessing);
    ForkProcessor forkProcessor = null;
    String delimiter = getDelimiter();
    if (delimiter == null) {
      forkProcessor = new ForkProcessor(routeContext.getCamelContext(), expression, threadPool, shutdownThreadPool, recipientList);
    } else {
      forkProcessor = new ForkProcessor(routeContext.getCamelContext(), expression, delimiter, threadPool, shutdownThreadPool, recipientList);
    }
    processors.set(1, forkProcessor);
    return Pipeline.newInstance(pipeline.getCamelContext(), processors);
  }
}
