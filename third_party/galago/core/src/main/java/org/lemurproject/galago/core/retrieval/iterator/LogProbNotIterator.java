/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Performs a log-space probablistic not operation
 *
 * returns log(1 - exp(p))
 *
 * @author sjh
 */
public class LogProbNotIterator extends TransformIterator implements ScoreIterator {

  private final ScoreIterator scorer;
  private final NodeParameters np;

  public LogProbNotIterator(NodeParameters params, ScoreIterator scorer) {
    super(scorer);
    this.scorer = scorer;
    this.np = params;
  }

  /**
   * Logically this node would actually identify all documents that are not a
   * candidate of it's child scorer. - To avoid scoring all documents, we assert
   * that this iterator scores all documents with non-background probabilities.
   */
  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public boolean hasMatch(ScoringContext context) {
    return true;
  }

  @Override
  public double score(ScoringContext c) {
    double score = scorer.score(c);
    // check if the score is a log-space probability:
    if (score < 0) {
      return Math.log(1 - Math.exp(score));
    }
    throw new RuntimeException("LogProbNot operator requires a log probability, for document: " + c.document + " iterator received: " + score);
  }

  @Override
  public double maximumScore() {
    if (scorer.maximumScore() < 0) {
      return Math.log(1 - Math.exp(scorer.maximumScore()));
    }
    return Double.MAX_VALUE;
  }

  @Override
  public double minimumScore() {
    if (scorer.minimumScore() < 0) {
      return Math.log(1 - Math.exp(scorer.minimumScore()));
    }
    return Double.MIN_VALUE;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = np.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
