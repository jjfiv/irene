// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * The internal iterator is expected to be an indicator iterator. This performs
 * a transform into the "score space" by emitting a boost score (beta) iff the
 * indicator is on.
 *
 * @author irmarc
 */
public class BoostingIterator extends TransformIterator implements ScoreIterator {

  NodeParameters p;
  double beta;

  public BoostingIterator(NodeParameters p, IndicatorIterator inner) {
    super(inner);
    this.p = p;
    beta = p.get("beta", 0.5);
  }

  @Override
  public double score(ScoringContext c) {
    if (hasMatch(c) && ((IndicatorIterator) iterator).indicator(c)) {
      return beta;
    } else {
      return 0.0;
    }
  }

  @Override
  public double maximumScore() {
    return beta;
  }

  @Override
  public double minimumScore() {
    return 0.0;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "score";
    String className = this.getClass().getSimpleName();
    String parameters = p.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = Double.toString(score(c));
    List<AnnotatedNode> children = Collections.singletonList(this.iterator.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
