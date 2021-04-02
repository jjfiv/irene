// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.StageInstanceDescription.PipeInput;
import org.lemurproject.galago.tupleflow.execution.StageInstanceDescription.PipeOutput;
import org.lemurproject.galago.tupleflow.runtime.FileOrderedReader;
import org.lemurproject.galago.tupleflow.runtime.FileOrderedWriter;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.utility.debug.NullCounter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author trevor
 */
public class StageInstanceFactory {

  NetworkedCounterManager counterManager;

  public StageInstanceFactory(NetworkedCounterManager counterManager) {
    this.counterManager = counterManager;
  }

  public class StepParameters implements TupleFlowParameters {

    Parameters params;
    StageInstanceDescription instance;

    public StepParameters(StepInformation o, StageInstanceDescription instance) {
      this.params = o.getParameters();
      this.instance = instance;
    }

    @Override
    public Counter getCounter(String name) {
      if (instance.getMasterURL() == null) {
        return NullCounter.instance;
      } else {
        return counterManager.newCounter(
                name, instance.getName(),
                Integer.toString(instance.getIndex()), instance.getMasterURL());
      }
    }

    @Override
    public TypeReader getTypeReader(String specification) throws IOException {
      PipeOutput pipeOutput = instance.getReaders().get(specification);
      return StageInstanceFactory.getTypeReader(pipeOutput);
    }

    @Override
    public Processor getTypeWriter(String specification) throws IOException {
      PipeInput pipeInput = instance.getWriters().get(specification);
      return StageInstanceFactory.getTypeWriter(pipeInput);
    }

    @Override
    public boolean readerExists(String specification, String className, String[] order) {
      return instance.readerExists(specification, className, order);
    }

    @Override
    public boolean writerExists(String specification, String className, String[] order) {
      return instance.writerExists(specification, className, order);
    }

    @Override
    public Parameters getJSON() {
      return params;
    }

    @Override
    public int getInstanceId() {
      return instance.index;
    }
  }

  public ExNihiloSource instantiate(StageInstanceDescription instance)
          throws IncompatibleProcessorException, IOException {
    return (ExNihiloSource) instantiate(instance, instance.getStage().getSteps());
  }

  public Step instantiate(
          StageInstanceDescription instance,
          List<StepInformation> steps)
          throws IncompatibleProcessorException, IOException {
    Step previous = null;
    Step first = null;

    for (StepInformation step : steps) {
      Step current;

      if (step instanceof MultiStepInformation) {
        current = instantiateMulti(instance, step);
      } else if (step instanceof InputStepInformation) {
        current = instantiateInput(instance, (InputStepInformation) step);
      } else if (step instanceof MultiInputStepInformation) {
        current = instantiateInput(instance, (MultiInputStepInformation) step);
      } else if (step instanceof OutputStepInformation) {
        current = instantiateOutput(instance, (OutputStepInformation) step);
      } else {
        current = instantiateStep(instance, step);
      }

      if (first == null) {
        first = current;
      }
      if (previous != null) {
        ((Source) previous).setProcessor(current);
      }

      previous = current;
    }

    return first;
  }

  public Step instantiateStep(
          StageInstanceDescription instance,
          final StepInformation step) throws IOException {
    Step object;

    try {
      Class objectClass = Class.forName(step.getClassName());
      Constructor parameterArgumentConstructor = null;
      Constructor noArgumentConstructor = null;

      for (Constructor c : objectClass.getConstructors()) {
        java.lang.reflect.Type[] parameters = c.getGenericParameterTypes();

        if (parameters.length == 0) {
          noArgumentConstructor = c;
        } else if (parameters.length == 1 && parameters[0] == TupleFlowParameters.class) {
          parameterArgumentConstructor = c;
        }
      }

      if (parameterArgumentConstructor != null) {
        object = (Step) parameterArgumentConstructor.newInstance(
                new StepParameters(step, instance));
      } else if (noArgumentConstructor != null) {
        object = (Step) noArgumentConstructor.newInstance();
      } else {
        throw new IncompatibleProcessorException(
                "Couldn't instantiate this class because "
                + "no compatible constructor was found: " + step.getClassName());
      }
    } catch (Exception e) {
      throw new IOException("Couldn't instantiate a step object: " + step.getClassName(), e);
    }

    return object;
  }

  public Step instantiateInput(
          StageInstanceDescription instance,
          InputStepInformation step) throws IOException {
    PipeOutput pipeOutput = instance.getReaders().get(step.getId());
    return getTypeReaderSource(pipeOutput);
  }

  public Step instantiateInput(
          StageInstanceDescription instance,
          MultiInputStepInformation step) throws IOException {
    String[] ids = step.getIds();
    PipeOutput[] pipes = new PipeOutput[ids.length];
    for (int i = 0; i < ids.length; i++) {
      pipes[i] = instance.getReaders().get(ids[i]);
    }
    return getTypeReaderSource(pipes);
  }

  public Step instantiateOutput(
          StageInstanceDescription instance,
          final OutputStepInformation step) throws IOException {
    PipeInput pipeInput = instance.getWriters().get(step.getId());
    return getTypeWriter(pipeInput);
  }

  private Step instantiateMulti(
          StageInstanceDescription instance,
          final StepInformation step) throws IncompatibleProcessorException, IOException {
    MultiStepInformation multiStep = (MultiStepInformation) step;
    Processor[] processors = new Processor[multiStep.size()];

    int i = 0;
    for (String groupName : multiStep) {
      processors[i] = 
	  (org.lemurproject.galago.tupleflow.Processor) instantiate(instance, 
								    multiStep.getGroup(groupName));
      ++i;
    }

    return new org.lemurproject.galago.tupleflow.Multi(processors);
  }

  protected static Order createOrder(final DataPipe pipe) throws IOException {
    return createOrder(pipe.className, pipe.order);
  }

  public static Order createOrder(String className, String[] orderSpec) throws IOException {
    Order order;

    try {
      Class typeClass = Class.forName(className);
      Type type = (Type) typeClass.getConstructor().newInstance();
      order = type.getOrder(orderSpec);
    } catch (Exception e) {
      throw new IOException("Couldn't create an order object for type: " + className, e);
    }

    return order;
  }

  // Returns a ReaderSource that reads from multiple named pipes
  public ReaderSource getTypeReaderSource(PipeOutput[] pipes) throws IOException {
    ReaderSource reader;

    if (pipes.length == 0) {
      return null;
    }

    // Creare our order and accumulate file names
    Order order = createOrder(pipes[0].getPipe());
    ArrayList<String> fileNames = new ArrayList<String>();
    for (PipeOutput po : pipes) {
      fileNames.addAll(Arrays.asList(po.getFileNames()));
    }

    if (fileNames.size() > 1) {
      reader = OrderedCombiner.combineFromFiles(fileNames, order);
    } else {
      reader = new FileOrderedReader(fileNames.get(0));
    }
    return reader;

  }

  public ReaderSource getTypeReaderSource(PipeOutput pipeOutput) throws IOException {
    ReaderSource reader;

    if (pipeOutput == null) {
      return null;
    }

    Order order = createOrder(pipeOutput.getPipe());
    String[] fileNames = pipeOutput.getFileNames();

    if (fileNames.length > 1) {
      reader = OrderedCombiner.combineFromFiles(Arrays.asList(fileNames), order);
    } else {
      reader = new FileOrderedReader(fileNames[0]);
    }
    return reader;
  }

  @SuppressWarnings(value = "unchecked")
  public static <T> ReaderSource<T> getTypeReader(final PipeOutput pipeOutput) throws IOException {
    ReaderSource<T> reader;

    if (pipeOutput == null) {
      return null;
    }

    Order order = createOrder(pipeOutput.getPipe());
    String[] fileNames = pipeOutput.getFileNames();

    if (fileNames.length > 100) {
      List<String> names = Arrays.asList(fileNames);
      ArrayList<String> reduced = new ArrayList<String>();

      // combine 20 files at a time
      for (int i = 0; i < names.size(); i += 20) {
        int start = i;
        int end = Math.min(names.size(), i + 20);
        List<String> toCombine = names.subList(start, end);

        OrderedCombiner combReader = OrderedCombiner.combineFromFiles(toCombine, order);
        reader = combReader;
        CompressionType c = combReader.getCompression();
        
        File temporary = FileUtility.createTemporary();
        FileOrderedWriter<T> writer = new FileOrderedWriter<T>(temporary.getAbsolutePath(), order, c);

        try {
          reader.setProcessor(writer);
        } catch (IncompatibleProcessorException e) {
          throw new IOException("Incompatible processor for reader tuples", e);
        }

        reader.run();

        reduced.add(temporary.toString());
        temporary.deleteOnExit();
      }

      reader = OrderedCombiner.combineFromFiles(reduced, order);
    } else if (fileNames.length > 1) {
      reader = OrderedCombiner.combineFromFiles(Arrays.asList(fileNames), order);
    } else {
      reader = new FileOrderedReader(fileNames[0]);
    }
    return reader;
  }

  public static Processor getTypeWriter(final PipeInput pipeInput) throws IOException, IOException {
    Processor writer;

    if (pipeInput == null) {
      return null;
    }
    String[] fileNames = pipeInput.getFileNames();
    Order order = createOrder(pipeInput.getPipe());
    Order hashOrder = createOrder(pipeInput.getPipe().getClassName(), pipeInput.getPipe().getHash());

    assert order != null : "Order not found: " + Arrays.toString(pipeInput.getPipe().getOrder());

    try {
      if (fileNames.length == 1) {
        writer = new FileOrderedWriter(fileNames[0], order, pipeInput.getPipe().getCompression());
      } else {
        assert hashOrder != null : "Hash order not found: " + pipeInput.getPipe().getPipeName() + " " + pipeInput.getPipe().getHash();
        writer = Splitter.splitToFiles(fileNames, order, hashOrder, pipeInput.getPipe().getCompression());
      }
    } catch (IncompatibleProcessorException e) {
      throw new IOException("Failed to create a typeWriter", e);
    }

    return writer;
  }
}
