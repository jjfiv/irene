// BSD License (http://lemurproject.org/galago-license)
/*
 * SparseFloatListReaderTest.java
 * JUnit based test
 *
 * Created on October 9, 2007, 1:00 PM
 */
package org.lemurproject.galago.core.retrieval;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.index.FakeLengthIterator;
import org.lemurproject.galago.core.index.disk.SparseFloatListReader;
import org.lemurproject.galago.core.index.disk.SparseFloatListWriter;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class SparseFloatListReaderTest {

  private File tempPath;
  int[] aDocs = new int[]{5, 6};
  float[] aScores = new float[]{0.5f, 0.7f};
  int[] bDocs = new int[]{9, 11, 13};
  float[] bScores = new float[]{0.1f, 0.2f, 0.3f};

  @Before
  public void setUp() throws IOException {
    // make a spot for the index
    tempPath = FileUtility.createTemporary();

    Parameters p = Parameters.create();
    p.set("filename", tempPath.toString());

    TupleFlowParameters parameters = new FakeParameters(p);
    SparseFloatListWriter writer = new SparseFloatListWriter(parameters);

    writer.processWord(ByteUtil.fromString("a"));

    for (int i = 0; i < aDocs.length; i++) {
      writer.processNumber(aDocs[i]);
      writer.processTuple(aScores[i]);
    }

    writer.processWord(ByteUtil.fromString("b"));

    for (int i = 0; i < bDocs.length; i++) {
      writer.processNumber(bDocs[i]);
      writer.processTuple(bScores[i]);
    }

    writer.close();
  }

  @After
  public void tearDown() throws IOException {
    tempPath.delete();
  }

  @Test
  public void testA() throws Exception {
    SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
    ScoreIterator iter = (ScoreIterator) instance.getIterator(new Node("scores", "a"));
    assertFalse(iter.isDone());
    int i;
    ScoringContext context = new ScoringContext();
    int[] lengths = new int[aDocs.length];
    Arrays.fill(lengths, 100);
    FakeLengthIterator fli = new FakeLengthIterator(aDocs, lengths);
    for (i = 0; !iter.isDone(); i++) {
      assertEquals(aDocs[i], iter.currentCandidate());
      context.document = aDocs[i];
      assertEquals(aScores[i], iter.score(context), 0.0001);
      assertTrue(iter.hasMatch(context));

      iter.movePast(aDocs[i]);
    }

    assertEquals(aDocs.length, i);
    assertTrue(iter.isDone());
  }

  @Test
  public void testB() throws Exception {
    SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
    ScoreIterator iter = (ScoreIterator) instance.getIterator(new Node("scores", "b"));
    int i;

    assertFalse(iter.isDone());
    ScoringContext ctx = new ScoringContext();
    int[] lengths = new int[bDocs.length];
    Arrays.fill(lengths, 100);
    FakeLengthIterator fli = new FakeLengthIterator(bDocs, lengths);
    for (i = 0; !iter.isDone(); i++) {
      assertEquals(bDocs[i], iter.currentCandidate());
      ctx.document = bDocs[i];
      assertEquals(bScores[i], iter.score(ctx), 0.0001);
      assertTrue(iter.hasMatch(ctx));

      iter.movePast(bDocs[i]);
    }

    assertEquals(bDocs.length, i);
    assertTrue(iter.isDone());
  }

  @Test
  public void testIterator() throws Exception {
    SparseFloatListReader instance = new SparseFloatListReader(tempPath.toString());
    SparseFloatListReader.KeyValueIterator iter = instance.getIterator();
    String term = iter.getKeyString();

    assertEquals(term, "a");
    assertFalse(iter.isDone());

    ScoreIterator lIter = (ScoreIterator) iter.getValueIterator();
    ScoringContext context = new ScoringContext();
    int[] lengths = new int[aDocs.length];
    Arrays.fill(lengths, 100);
    FakeLengthIterator fli = new FakeLengthIterator(aDocs, lengths);
    for (int i = 0; !lIter.isDone(); i++) {
      assertEquals(lIter.currentCandidate(), aDocs[i]);
      context.document = aDocs[i];
      assertEquals(lIter.score(context), aScores[i], 0.0001);
      assertTrue(lIter.hasMatch(context));

      lIter.movePast(aDocs[i]);
    }

    assertTrue(iter.nextKey());
    term = iter.getKeyString();
    assertEquals(term, "b");
    assertFalse(iter.isDone());
    lIter = (ScoreIterator) iter.getValueIterator();

    context = new ScoringContext();
    lengths = new int[bDocs.length];
    Arrays.fill(lengths, 100);
    fli = new FakeLengthIterator(bDocs, lengths);
    for (int i = 0; !lIter.isDone(); i++) {
      assertEquals(lIter.currentCandidate(), bDocs[i]);
      context.document = lIter.currentCandidate();
      assertEquals(lIter.score(context), bScores[i], 0.0001);
      assertTrue(lIter.hasMatch(context));

      lIter.movePast(bDocs[i]);
    }
    assertTrue(lIter.isDone());
    assertFalse(iter.nextKey());
  }
}
