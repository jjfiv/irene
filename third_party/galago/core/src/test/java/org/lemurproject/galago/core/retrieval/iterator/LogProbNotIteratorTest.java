/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class LogProbNotIteratorTest {

  @Test
  public void testLogProbNot() throws IOException {
    final int[] docs = new int[]{1, 2, 3, 4, 5, 10};
    final double[] scores = new double[]{-0.1, -0.2, -0.3, -0.4, -0.5, -0.6};

    FakeScorer child = new FakeScorer(docs, scores);
    LogProbNotIterator scorer = new LogProbNotIterator(new NodeParameters(), child);
    ScoringContext sc = new ScoringContext();

    double[] expected = new double[]{
      -2.352168,
      -1.707771,
      -1.350225,
      -1.109632,
      -0.932752,
      -0.795870};

    int i = 0;
    while (!scorer.isDone()) {
      sc.document = scorer.currentCandidate();
      assertTrue(scorer.hasMatch(sc));
      assertEquals(scorer.score(sc), expected[i], 0.00001);
      scorer.movePast(sc.document);
      i += 1;
    }

  }

  public class FakeScorer implements ScoreIterator {

    private final int[] docs;
    private final double[] scores;
    private int index;
    private ScoringContext context;
    private boolean done;

    public FakeScorer(int[] docs, double[] scores) {
      this.docs = docs;
      this.scores = scores;
      this.index = 0;
      this.done = false;
    }

    @Override
    public double score(ScoringContext c) {
      if (c.document == docs[index]) {
        return scores[index];
      }
      return Utility.tinyLogProbScore;
    }

    @Override
    public double maximumScore() {
      return 0;
    }

    @Override
    public double minimumScore() {
      return Utility.tinyLogProbScore;
    }

    @Override
    public void reset() throws IOException {
      index = 0;
    }

    @Override
    public long currentCandidate() {
      return docs[index];
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public void movePast(long identifier) throws IOException {
      syncTo(identifier + 1);
    }

    @Override
    public void syncTo(long identifier) throws IOException {
      while (index < docs.length - 1 && docs[index] < identifier) {
        index += 1;
      }
      if (docs[index] < identifier) {
        done = true;
      }
    }

    @Override
    public boolean hasMatch(ScoringContext context) {
      return !done && docs[index] == context.document;
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    @Override
    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getValueString(ScoringContext sc) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

  }
}
