// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.extents;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.UnorderedWindowIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 *
 * @author trevor
 */
public class UnorderedWindowIteratorTest {

  @Test
  public void testPhrase() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters twoParam = new NodeParameters();
    twoParam.set("default", 2);
    UnorderedWindowIterator instance = new UnorderedWindowIterator(twoParam, iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();

    ExtentArray array = instance.extents(context);
    assertFalse(instance.isDone());

    assertEquals(1, array.size());
    assertEquals(1, array.getDocument());
    assertEquals(3, array.begin(0));
    assertEquals(5, array.end(0));

    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertTrue(instance.isDone());
  }

  @Test
  public void testUnordered() throws IOException {
    int[][] dataOne = {{1, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters twoParam = new NodeParameters();
    twoParam.set("default", 2);
    UnorderedWindowIterator instance = new UnorderedWindowIterator(twoParam, iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();

    ExtentArray array = instance.extents(context);
    assertFalse(instance.isDone());

    assertEquals(array.size(), 1);
    assertEquals(1, array.getDocument());
    assertEquals(array.begin(0), 3);
    assertEquals(array.end(0), 5);

    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertTrue(instance.isDone());
  }

  @Test
  public void testDifferentDocuments() throws IOException {
    int[][] dataOne = {{2, 3}};
    int[][] dataTwo = {{1, 4}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters twoParam = new NodeParameters();
    twoParam.set("default", 2);

    UnorderedWindowIterator instance = new UnorderedWindowIterator(twoParam, iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();

    ExtentArray array = instance.extents(context);
    assertEquals(0, array.size());
    assertTrue(!instance.isDone());

    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertTrue(instance.isDone());
  }

  @Test
  public void testMultipleDocuments() throws IOException {
    int[][] dataOne = {{1, 3}, {2, 5}, {5, 11}};
    int[][] dataTwo = {{1, 4}, {3, 8}, {5, 9}};
    FakeExtentIterator one = new FakeExtentIterator(dataOne);
    FakeExtentIterator two = new FakeExtentIterator(dataTwo);
    FakeExtentIterator[] iters = {one, two};

    NodeParameters fiveParam = new NodeParameters();
    fiveParam.set("width", 5);

    UnorderedWindowIterator instance = new UnorderedWindowIterator(fiveParam, iters);

    ScoringContext context = new ScoringContext();

    context.document = instance.currentCandidate();

    ExtentArray array = instance.extents(context);
    assertFalse(instance.isDone());

    assertEquals(array.size(), 1);
    assertEquals(array.getDocument(), 1);
    assertEquals(array.begin(0), 3);
    assertEquals(array.end(0), 5);

    // move to 2
    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertFalse(instance.isDone());
    assertFalse(instance.hasMatch(new ScoringContext(2)));

    // move to 4
    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertFalse(instance.isDone());
    assertFalse(instance.hasMatch(new ScoringContext(4)));

    // move to 5
    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertFalse(instance.isDone());
    assertTrue(instance.hasMatch(new ScoringContext(5)));

    array = instance.extents(context);
    assertEquals(array.size(), 1);
    assertEquals(array.getDocument(), 5);
    assertEquals(array.begin(0), 9);
    assertEquals(array.end(0), 12);

    instance.movePast(instance.currentCandidate());
    context.document = instance.currentCandidate();

    assertTrue(instance.isDone());
  }
}
