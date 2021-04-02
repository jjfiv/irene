// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author trevor
 */
public class ScaleIterator extends TransformIterator implements ScoreIterator {

  NodeParameters np;
  double weight;

  public ScaleIterator(NodeParameters parameters, ScoreIterator iterator) throws IllegalArgumentException {
    super(iterator);
    this.np = parameters;
    weight = parameters.get("default", 1.0);
  }

  public double score(ScoringContext c) {
    return weight * ((ScoreIterator) iterator).score(c);
  }

  public double maximumScore() {
    return weight * ((ScoreIterator) iterator).maximumScore();
  }

  public double minimumScore() {
    return weight * ((ScoreIterator) iterator).minimumScore();
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
