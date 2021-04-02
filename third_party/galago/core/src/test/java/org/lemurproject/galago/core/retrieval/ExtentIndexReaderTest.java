// BSD License (http://lemurproject.org/galago-license)
/*
 * ExtentIndexReaderTest.java
 * JUnit based test
 *
 * Created on October 5, 2007, 4:36 PM
 */
package org.lemurproject.galago.core.retrieval;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.utility.btree.disk.DiskBTreeReader;
import org.lemurproject.galago.core.index.disk.WindowIndexReader;
import org.lemurproject.galago.core.index.disk.WindowIndexWriter;
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class ExtentIndexReaderTest {

  File tempPath;

  @Before
  public void setUp() throws Exception {
    // make a spot for the index
    tempPath = FileUtility.createTemporary();
    tempPath.delete();

    Parameters p = Parameters.create();
    p.set("filename", tempPath.toString());

    WindowIndexWriter writer =
            new WindowIndexWriter(new org.lemurproject.galago.tupleflow.FakeParameters(p));

    writer.processExtentName(ByteUtil.fromString("title"));
    writer.processNumber(1);
    writer.processBegin(2);
    writer.processTuple(3);
    writer.processBegin(10);
    writer.processTuple(11);

    writer.processNumber(9);
    writer.processBegin(5);
    writer.processTuple(10);

    writer.processExtentName(ByteUtil.fromString("z"));
    writer.processNumber(15);
    writer.processBegin(9);
    writer.processTuple(11);

    writer.close();
  }

  @After
  public void tearDown() throws Exception {
    if (tempPath != null) {
      tempPath.delete();
    }
  }

  @Test
  public void testReadTitle() throws Exception {
    WindowIndexReader reader = new WindowIndexReader(new DiskBTreeReader(tempPath.toString()));
    ExtentIterator extents = reader.getTermExtents("title");
    ScoringContext sc = new ScoringContext();
    
    assertFalse(extents.isDone());

    assertEquals(1, extents.currentCandidate());
    sc.document = extents.currentCandidate();
    ExtentArray e = extents.extents(sc);
    assertEquals(2, e.size());
    ExtentArrayIterator iter = new ExtentArrayIterator(e);
    assertFalse(iter.isDone());

    assertEquals(2, iter.currentBegin());
    assertEquals(3, iter.currentEnd());

    iter.next();
    assertFalse(iter.isDone());

    assertEquals(10, iter.currentBegin());
    assertEquals(11, iter.currentEnd());

    iter.next();
    assertTrue(iter.isDone());

    extents.movePast(extents.currentCandidate());
    assertFalse(extents.isDone());

    assertEquals(9, extents.currentCandidate());
    sc.document = extents.currentCandidate();
    e = extents.extents(sc);
    iter = new ExtentArrayIterator(e);

    assertEquals(5, iter.currentBegin());
    assertEquals(10, iter.currentEnd());

    extents.movePast(extents.currentCandidate());
    assertTrue(extents.isDone());

    reader.close();
  }

  @Test
  public void testReadZ() throws Exception {
    WindowIndexReader reader = new WindowIndexReader(new DiskBTreeReader(tempPath.toString()));
    ExtentIterator extents = reader.getTermExtents("z");
    ScoringContext sc = new ScoringContext();

    assertFalse(extents.isDone());

    assertEquals(15, extents.currentCandidate());
    sc.document = extents.currentCandidate();
    ExtentArray e = extents.extents(sc);
    ExtentArrayIterator iter = new ExtentArrayIterator(e);

    assertEquals(9, iter.currentBegin());
    assertEquals(11, iter.currentEnd());

    extents.movePast(extents.currentCandidate());
    assertTrue(extents.isDone());

    reader.close();
  }

  @Test
  public void testSimpleSkipTitle() throws Exception {
    WindowIndexReader reader = new WindowIndexReader(new DiskBTreeReader(tempPath.toString()));
    ExtentIterator extents = reader.getTermExtents("title");
    ScoringContext sc = new ScoringContext();

    assertFalse(extents.isDone());
    extents.syncTo(10);
    assertTrue(extents.isDone());

    reader.close();
  }

  @Test
  public void testSkipList() throws Exception {
    Parameters p = Parameters.create();
    p.set("filename", tempPath.toString());
    p.set("skipDistance", 10);

    WindowIndexWriter writer =
            new WindowIndexWriter(new org.lemurproject.galago.tupleflow.FakeParameters(p));

    writer.processExtentName(ByteUtil.fromString("skippy"));
    for (int docid = 1; docid < 1000; docid += 3) {
      writer.processNumber(docid);
      for (int begin = 5; begin < (20 + (docid / 5)); begin += 4) {
        writer.processBegin(begin);
        writer.processTuple(begin + 2);
      }
    }
    writer.close();

    WindowIndexReader reader = new WindowIndexReader(new DiskBTreeReader(tempPath.toString()));
    ExtentIterator extents = reader.getTermExtents("skippy");
    ScoringContext sc = new ScoringContext();

    assertFalse(extents.isDone());
    extents.syncTo(453);
    assertFalse(extents.hasMatch(new ScoringContext(453)));
    assertEquals(454, extents.currentCandidate());
    extents.movePast(extents.currentCandidate());
    assertEquals(457, extents.currentCandidate());
    sc.document = extents.currentCandidate();
    assertEquals(27, extents.count(sc));
    ExtentArray ea = extents.extents(sc);
    ExtentArrayIterator eait = new ExtentArrayIterator(ea);
    int begin = 5;
    while (!eait.isDone()) {
      assertEquals(begin, eait.currentBegin());
      assertEquals(begin + 2, eait.currentEnd());
      begin += 4;
      eait.next();
    }
    extents.syncTo(1299);
    assertFalse(extents.hasMatch(new ScoringContext(1299)));
    extents.movePast(2100);
    assertTrue(extents.isDone());
    reader.close();
  }
}
