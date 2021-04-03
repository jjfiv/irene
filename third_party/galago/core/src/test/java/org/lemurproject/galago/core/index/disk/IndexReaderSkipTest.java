/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.index.disk;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class IndexReaderSkipTest {

  @Test
  public void testPositionIndexSkipping() throws Exception {
    File temp = FileUtility.createTemporary();

    try {
      Parameters parameters = Parameters.create();
      parameters.set("filename", temp.getAbsolutePath());
      parameters.set("skipDistance", 10);
      parameters.set("skipResetDistance", 5);
      PositionIndexWriter writer = new PositionIndexWriter(new FakeParameters(parameters));

      writer.processWord(ByteUtil.fromString("key"));
      for (int doc = 0; doc < 1000; doc++) {
        writer.processDocument(doc);
        for (int begin = 0; begin < 100; begin++) {
          writer.processPosition(begin);
        }
      }
      writer.close();

      PositionIndexReader reader = new PositionIndexReader(parameters.getString("filename"));
      for (int i = 1; i < 1000; i++) {
        BaseIterator extents = reader.getTermExtents("key");
        extents.syncTo(i);
        assertEquals(extents.currentCandidate(), i);

        BaseIterator counts = reader.getTermCounts("key");
        counts.syncTo(i);
        assertEquals(counts.currentCandidate(), i);
      }
      reader.close();
    } finally {
      assertTrue(temp.delete());
    }
  }

  @Test
  public void testCountIndexSkipping() throws Exception {
    Random r = new Random();
    File temp = FileUtility.createTemporary();

    try {
      Parameters parameters = Parameters.create();
      parameters.set("filename", temp.getAbsolutePath());
      parameters.set("skipDistance", 10);
      parameters.set("skipResetDistance", 5);
      CountIndexWriter writer = new CountIndexWriter(new FakeParameters(parameters));

      writer.processWord(ByteUtil.fromString("key"));
      for (int doc = 0; doc < 1000; doc++) {
        writer.processDocument(doc);
        writer.processTuple(r.nextInt(128) + 128);
      }
      writer.close();

      CountIndexReader reader = new CountIndexReader(parameters.getString("filename"));
      for (int i = 1; i < 1000; i++) {
        BaseIterator counts = reader.getTermCounts("key");
        counts.syncTo(i);
        assertEquals(counts.currentCandidate(), i);
      }
      reader.close();
      
    } finally {
      assertTrue(temp.delete());
    }
  }

  @Test
  public void testWindowIndexSkipping() throws Exception {
    Random r = new Random();
    File temp = FileUtility.createTemporary();

    try {
      Parameters parameters = Parameters.create();
      parameters.set("filename", temp.getAbsolutePath());
      parameters.set("skipDistance", 10);
      parameters.set("skipResetDistance", 5);
      WindowIndexWriter writer = new WindowIndexWriter(new FakeParameters(parameters));

      writer.processExtentName(ByteUtil.fromString("key"));
      for (int doc = 0; doc < 1000; doc++) {
        writer.processNumber(doc);
        for (int begin = 0; begin < 100; begin++) {
          writer.processBegin(begin);
          writer.processTuple(begin + r.nextInt(128) + 128); // end is between 128 and 256 after begin
        }
      }
      writer.close();

      WindowIndexReader reader = new WindowIndexReader(parameters.getString("filename"));
      for (int i = 1; i < 1000; i++) {
        BaseIterator extents = reader.getTermExtents("key");
        extents.syncTo(i);
        assertEquals(extents.currentCandidate(), i);

        BaseIterator counts = reader.getTermCounts("key");
        counts.syncTo(i);
        assertEquals(counts.currentCandidate(), i);
      }
      reader.close();
      
    } finally {
      assertTrue(temp.delete());
    }
  }
}
