// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.utility.btree.GenericElement;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.core.types.DocumentFeature;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.IOException;

/**
 * Writes the document indicator file 
 * 
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.DocumentFeature", order = {"+document"})
public class DocumentPriorWriter extends KeyValueWriter<DocumentFeature> {

  int lastDocument = -1;
  double maxObservedScore = Double.NEGATIVE_INFINITY;
  double minObservedScore = Double.POSITIVE_INFINITY;
  Counter written;

  /** Creates a new create of DocumentLengthsWriter */
  public DocumentPriorWriter(TupleFlowParameters parameters) throws IOException {
    super(parameters, "Document indicators written");
    Parameters p = writer.getManifest();
    p.set("writerClass", DocumentPriorWriter.class.getName());
    p.set("readerClass", DocumentPriorReader.class.getName());

    written = parameters.getCounter("Priors Written");
  }

  @Override
  public GenericElement prepare(DocumentFeature docfeat) throws IOException {
    // word is ignored
    assert ((lastDocument < 0) || (lastDocument < docfeat.document)) : "DocumentPriorWriter keys must be unique and in sorted order.";

    maxObservedScore = Math.max(maxObservedScore, docfeat.value);
    minObservedScore = Math.min(minObservedScore, docfeat.value);
    GenericElement element = new GenericElement(Utility.fromLong(docfeat.document), Utility.fromDouble(docfeat.value));

    written.increment();
    return element;
  }

  @Override
  public void close() throws IOException {
    Parameters p = writer.getManifest();
    p.set("maxScore", this.maxObservedScore);
    p.set("minScore", this.minObservedScore);
    if(!p.isDouble("default")){
      p.set("defaultProb", this.minObservedScore);
    }
    super.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("KeyValueWriters require a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }
}
