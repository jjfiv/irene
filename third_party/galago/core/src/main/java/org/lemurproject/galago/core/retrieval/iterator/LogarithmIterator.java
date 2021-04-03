/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Not implemented to work over raw counts because it shouldn't blindly be
 * applied to counts - doing that can result in -Infinity scores everywhere.
 * Therefore, this applies to scores, meaning to you had to make a conscious
 * decision to pass a raw count up, and if things go wrong, it's your fault.
 *
 * @author irmarc
 */
public class LogarithmIterator extends TransformIterator implements ScoreIterator {

  NodeParameters np;
  ScoreIterator scorer;

  public LogarithmIterator(NodeParameters params, ScoreIterator svi) {
    super(svi);
    this.np = params;
    scorer = svi;
  }

  @Override
  public double score(ScoringContext c) {
    return Math.log(scorer.score(c));
  }

  @Override
  public double maximumScore() {
    return Math.log(scorer.maximumScore());
  }

  @Override
  public double minimumScore() {
    return Math.log(scorer.minimumScore());
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
