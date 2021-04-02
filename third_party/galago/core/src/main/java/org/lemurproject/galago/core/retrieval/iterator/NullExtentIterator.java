// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.ExtentArray;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author trevor
 * @author irmarc
 */
public class NullExtentIterator implements ExtentIterator, CountIterator {

  ExtentArray array = new ExtentArray();

  public NullExtentIterator() {
  }

  public NullExtentIterator(NodeParameters p) {
    // nothing
  }

  public boolean nextEntry() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public ExtentArray extents(ScoringContext c) {
    return array;
  }

  @Override
  public int count(ScoringContext c) {
    return 0;
  }

  @Override
  public void reset() {
    // do nothing
  }

  @Override
  public ExtentArray data(ScoringContext c) {
    return array;
  }

  @Override
  public long totalEntries() {
    return 0;
  }

  @Override
  public long currentCandidate() {
    return Long.MAX_VALUE;
  }

  @Override
  public boolean hasMatch(ScoringContext id) {
    return false;
  }

  @Override
  public String getValueString(ScoringContext c) throws IOException {
    return "NULL";
  }

  @Override
  public void syncTo(long identifier) throws IOException {
  }

  @Override
  public void movePast(long identifier) throws IOException {
  }

  @Override
  public int compareTo(@Nonnull BaseIterator t) {
    // hacked to always be shifted to the end.
    return 1;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) {
    String type = "extent";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c);
    String returnValue = array.toString();
    List<AnnotatedNode> children = Collections.emptyList();
    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }

  @Override
  public boolean indicator(ScoringContext c) {
    return false;
  }
}
