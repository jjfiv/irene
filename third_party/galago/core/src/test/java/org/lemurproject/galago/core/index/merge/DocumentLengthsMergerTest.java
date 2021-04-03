/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import org.junit.Test;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.DiskLengthsReader;
import org.lemurproject.galago.core.index.disk.DiskLengthsWriter;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.core.util.DocumentSplitFactory;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.HashMap;

import static org.junit.Assert.*;

/**
 *
 * @author sjh
 */
public class DocumentLengthsMergerTest {
  private static String makeLengthsIndex(int firstDocNum, File folder) throws Exception {
    File temp = new File(folder + File.separator + "lengths");
    Parameters p = Parameters.create();
    p.set("filename", temp.getAbsolutePath());
    DiskLengthsWriter writer = new DiskLengthsWriter(new FakeParameters(p));

    byte[] key = ByteUtil.fromString("document");
    for (int i = firstDocNum; i < firstDocNum + 100; i++) {
      writer.process(new FieldLengthData(key, i, i + 1));
    }

    writer.close();

    return temp.getAbsolutePath();
  }

  @Test
  public void testMerge1() throws Exception {
    // make two or three doclengths files

    File folder1 = null;
    File folder2 = null;
    String output = null;

    try {

      folder1 = FileUtility.createTemporaryDirectory();
      folder2 = FileUtility.createTemporaryDirectory();
      output = FileUtility.createTemporary().getAbsolutePath();

      String index1 = makeLengthsIndex(0, folder1);
      String index2 = makeLengthsIndex(100, folder2);

      IndexPartReader reader1 = DiskIndex.openIndexPart(index1);
      IndexPartReader reader2 = DiskIndex.openIndexPart(index2);

      HashMap<IndexPartReader, Integer> indexPartReaders = new HashMap<IndexPartReader, Integer>();
      indexPartReaders.put(reader1, 1);
      indexPartReaders.put(reader2, 2);

      String writerClassName = reader1.getManifest().getString("writerClass");
      String mergeClassName = reader1.getManifest().getString("mergerClass");

      Parameters p = Parameters.create();
      p.set("writerClass", writerClassName);
      p.set("filename", output);

      Class m = Class.forName(mergeClassName);
      @SuppressWarnings("unchecked")
      Constructor c = m.getConstructor(TupleFlowParameters.class);
      GenericIndexMerger merger = (GenericIndexMerger) c.newInstance(new FakeParameters(p));

      merger.setDocumentMapping(new DocumentMappingReader());

      merger.setInputs(indexPartReaders);
      merger.performKeyMerge();
      merger.close();

      // test that there are 100 keys and values.
      DiskLengthsReader tester = new DiskLengthsReader(output);
      LengthsIterator iterator = tester.getLengthsIterator();
      ScoringContext sc = new ScoringContext();
      while (!iterator.isDone()) {
        sc.document = iterator.currentCandidate();
        assertEquals(iterator.currentCandidate() + 1, iterator.length(sc));
        iterator.movePast(iterator.currentCandidate());
      }
      tester.close();

    } finally {

      if (folder1 != null) {
        FSUtil.deleteDirectory(folder1);
      }
      if (folder2 != null) {
        FSUtil.deleteDirectory(folder2);
      }
      if (output != null) {
        assertTrue(new File(output).delete());
      }
    }
  }

  @Test
  public void testMerge2() throws Exception {
    // make two or three doclengths files
    File indexFolder1 = null;
    File indexFolder2 = null;
    String output = null;

    try {
      indexFolder1 = FileUtility.createTemporaryDirectory();
      indexFolder2 = FileUtility.createTemporaryDirectory();

      String index1 = makeLengthsIndex(0, indexFolder1);
      String index2 = makeLengthsIndex(100, indexFolder2);

      assertNotNull(index1);
      assertNotNull(index2);

      output = FileUtility.createTemporary().getAbsolutePath();

      Parameters p = Parameters.create();
      p.set("part", "lengths");
      p.set("filename", output);
      IndexPartMergeManager manager = new IndexPartMergeManager(new FakeParameters(p));

      // add indexes to be merged
      manager.process(DocumentSplitFactory.numberedFile(indexFolder1.getAbsolutePath(), 2, 2));
      manager.process(DocumentSplitFactory.numberedFile(indexFolder2.getAbsolutePath(), 1, 2));

      // perform merge
      manager.close();

      // test that there are 100 keys and values.
      DiskLengthsReader tester = new DiskLengthsReader(output);
      LengthsIterator iterator = tester.getLengthsIterator();
      ScoringContext sc = new ScoringContext();
      while (!iterator.isDone()) {
        sc.document = iterator.currentCandidate();
        assertEquals(sc.document + 1, iterator.length(sc));
        iterator.movePast(sc.document);
      }

      tester.close();
      
    } finally {

      if (indexFolder1 != null) {
        FSUtil.deleteDirectory(indexFolder1);
      }
      if (indexFolder2 != null) {
        FSUtil.deleteDirectory(indexFolder2);
      }
      if (output != null) {
        assertTrue(new File(output).delete());
      }
    }
  }
}
