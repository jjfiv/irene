/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author sjh
 */
public class DisjointExtentsIterator extends DisjointIndexesIterator implements ExtentIterator, CountIterator {

  public DisjointExtentsIterator(Collection<ExtentIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int count(ScoringContext c) {
    return ((CountIterator) head).count(c);
  }

  @Override
  public ExtentArray extents(ScoringContext c) {
    return ((ExtentIterator) head).extents(c);
  }

  @Override
  public ExtentArray data(ScoringContext c) {
    return ((ExtentIterator) head).data(c);
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "extents";
    String className = this.getClass().getSimpleName();
    String parameters = this.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = extents(c).toString();
    List<AnnotatedNode> children = new ArrayList<>();
    for (BaseIterator child : this.allIterators) {
      children.add(child.getAnnotatedNode(c));
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public boolean indicator(ScoringContext c) {
    return count(c) > 0;
  }
}
