/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;

/**
 *
 * @author sjh
 */
public class ExtentMergeTest {
  private static void makeExtentsIndex(int offset, File folder) throws Exception {
    File temp = new File(folder + File.separator + "extents");
    Parameters p = Parameters.create();
    p.set("filename", temp.getAbsolutePath());
    WindowIndexWriter writer = new WindowIndexWriter(new FakeParameters(p));

    for(String word : new String[]{"word1","word2"}) {
      writer.processExtentName(ByteUtil.fromString(word));
      for(int doc = offset ; doc < offset+50 ; doc+=10) { // 100, 110, 120...
        writer.processNumber(doc);
        for(int begin = offset+5 ; begin < offset+50 ; begin+=10){ //105, 115, 125...
          writer.processBegin(begin);
          writer.processTuple(begin + 1); // 106 116 126
          writer.processTuple(begin + 2); // 107 117 127
        }
      }
    }

    writer.close();
  }

  @Test
  public void testExtentIndexMerger() throws Exception {

    File index1 = null;
    File index2 = null;
    File index3 = null;
    File output = null;
    try {
      index1 = FileUtility.createTemporaryDirectory();
      index2 = FileUtility.createTemporaryDirectory();
      index3 = FileUtility.createTemporaryDirectory();
      output = FileUtility.createTemporary();

      // three 10 document indexes (0 -> 9)
      makeExtentsIndex(100, index1);
      makeExtentsIndex(200, index2);
      makeExtentsIndex(300, index3);

      Parameters p = Parameters.create();
      p.set("filename", output.getAbsolutePath());
      p.set("writerClass", WindowIndexWriter.class.getName());
      /*ExtentIndexMerger merger = new ExtentIndexMerger(new FakeParameters(p));

      merger.setDocumentMapping(null);

      HashMap<StructuredIndexPartReader, Integer> inputs = new HashMap();
      inputs.put( StructuredIndex.openIndexPart(index1.getAbsolutePath()) , 1);
      inputs.put( StructuredIndex.openIndexPart(index2.getAbsolutePath()) , 2);
      inputs.put( StructuredIndex.openIndexPart(index3.getAbsolutePath()) , 3);
      merger.setInputs( inputs );
      merger.performKeyMerge();
      merger.close();

      // testing
      ExtentIndexReader reader = (ExtentIndexReader) StructuredIndex.openIndexPart(index3.getAbsolutePath());
      KeyIterator iterator = reader.getIterator();
      while(!iterator.isDone()){
        System.err.println( iterator.getKey() );

      }
       *
       */

    } finally {
      if (index1 != null) {
        FSUtil.deleteDirectory(index1);
      }
      if (index2 != null) {
        FSUtil.deleteDirectory(index2);
      }
      if (index3 != null) {
        FSUtil.deleteDirectory(index3);
      }
      if (output != null) {
        FSUtil.deleteDirectory(output);
      }
    }
  }
}
