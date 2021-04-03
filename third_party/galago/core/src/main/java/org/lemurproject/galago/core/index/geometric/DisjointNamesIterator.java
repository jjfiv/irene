/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 * @author sjh
 */
public class DisjointNamesIterator extends DisjointIndexesIterator implements DataIterator<String> {

  public DisjointNamesIterator(Collection<DataIterator<String>> iterators) {
    super((Collection) iterators);
  }

  @Override
  public String data(ScoringContext c) {
    if (head != null) {
      return ((DataIterator<String>) this.head).data(c);
    } else {
      throw new RuntimeException("Names Iterator is done.");
    }
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "counts";
    String className = this.getClass().getSimpleName();
    String parameters = this.toString();
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = data(c);
    List<AnnotatedNode> children = new ArrayList<>();
    for (BaseIterator child : this.allIterators) {
      children.add(child.getAnnotatedNode(c));
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
