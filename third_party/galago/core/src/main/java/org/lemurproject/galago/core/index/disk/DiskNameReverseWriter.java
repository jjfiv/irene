// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.btree.format.TupleflowDiskBTreeWriter;
import org.lemurproject.galago.utility.btree.GenericElement;
import org.lemurproject.galago.core.index.merge.DocumentNameReverseMerger;
import org.lemurproject.galago.core.types.DocumentNameId;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * 
 * Writes a mapping from document names to document numbers
 * 
 * Does not assume that the data is sorted
 *  - as data would need to be sorted into both key and value order
 *  - instead this class takes care of the re-sorting
 *  - this may be inefficient, but docnames is a relatively small pair of files
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.DocumentNameId", order = {"+name"})
public class DiskNameReverseWriter implements Processor<DocumentNameId> {

  TupleflowDiskBTreeWriter writer;
  DocumentNameId last = null;
  Counter documentNamesWritten;

  public DiskNameReverseWriter(TupleFlowParameters parameters) throws IOException {
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    String filename = parameters.getJSON().getString("filename");

    Parameters p = parameters.getJSON();
    p.set("writerClass", DiskNameReverseWriter.class.getName());
    p.set("mergerClass", DocumentNameReverseMerger.class.getName());
    p.set("readerClass", DiskNameReverseReader.class.getName());

    writer = new TupleflowDiskBTreeWriter(filename, p);
  }

  @Override
  public void process(DocumentNameId ndd) throws IOException {
    if (last == null) {
      last = ndd;
    } else {
      // ensure that we have an ident
      assert ndd.name != null: "DiskNameReverseWriter can not write a null identifier.";
      assert CmpUtil.compare(last.name, ndd.name) <= 0: "DiskNameReverseWriter wrong order.";
      if(CmpUtil.equals(last.name, ndd.name)) {
        Logger.getLogger(this.getClass().getName()).warning("identical document names written to names.reverse index: last="+ ByteUtil.toString(last.name)+" cur="+ByteUtil.toString(ndd.name));
      }
    }
    
    writer.add(new GenericElement(ndd.name, Utility.fromLong(ndd.id)));
    documentNamesWritten.increment();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("DocumentNameWriter requires a 'filename' parameter.");
      return;
    }
  }
}
