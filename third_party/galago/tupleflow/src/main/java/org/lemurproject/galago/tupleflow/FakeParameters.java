// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.utility.debug.NullCounter;

import java.io.IOException;

/**
 *
 * @author trevor
 */
public class FakeParameters implements TupleFlowParameters {
    Parameters parameters;

    public FakeParameters(Parameters p) {
        this.parameters = p;
    }

    public FakeParameters() {
      this(Parameters.create());
    }

    public Parameters getJSON() {
        return parameters;
    }
    
    public Counter getCounter(String name) {
        return NullCounter.instance;
    }

    public TypeReader getTypeReader(String specification) throws IOException {
        return null;
    }

    public Processor getTypeWriter(String specification) throws IOException {
        return null;
    }

    public boolean readerExists(String specification, String className, String[] order) {
        return false;
    }

    public boolean writerExists(String specification, String className, String[] order) {
        return false;
    }

    public int getInstanceId() {
        return 0;
    }
}
