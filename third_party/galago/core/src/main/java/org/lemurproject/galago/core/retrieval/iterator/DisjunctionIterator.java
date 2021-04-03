/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

import java.io.IOException;

/**
 *
 * @author sjh
 */
public abstract class DisjunctionIterator implements BaseIterator {

  protected BaseIterator[] iterators;
  protected BaseIterator[] drivingIterators;
  protected boolean hasAllCandidates;

  public DisjunctionIterator(BaseIterator[] queryIterators) {
    // first check that the iterators are all BaseIterators:
    this.iterators = queryIterators;

    // count the number of iterators that dont have
    // a non-default data for all candidates
    int drivingIteratorCount = 0;
    for (BaseIterator iterator : this.iterators) {
      if (!iterator.hasAllCandidates()) {
        drivingIteratorCount++;
      }
    }

    if (drivingIteratorCount == 0) {
      // if all iterators will report matches for all documents
      // make sure this information is communicated up.
      hasAllCandidates = true;
      drivingIterators = iterators;

    } else {
      // otherwise this disjunction is discriminative
      // and will not report matches for all documents
      //
      // the driving iterators will ensure this iterator
      //   does not stop at all documents
      hasAllCandidates = false;
      drivingIterators = new BaseIterator[drivingIteratorCount];
      int i = 0;
      for (BaseIterator iterator : this.iterators) {
        if (!iterator.hasAllCandidates()) {
          drivingIterators[i] = iterator;
          i++;
        }
      }
    }
  }

  @Override
  public void syncTo(long candidate) throws IOException {
    for (BaseIterator iterator : iterators) {
      iterator.syncTo(candidate);
    }
  }

  @Override
  public void movePast(long candidate) throws IOException {
    for (BaseIterator iterator : this.drivingIterators) {
      iterator.movePast(candidate);
    }
  }

  @Override
  public long currentCandidate() {
    // the current candidate is the smallest of the set
    long candidate = Long.MAX_VALUE;
    for (int i = 0; i < drivingIterators.length; i++) {
      if (!drivingIterators[i].isDone()) {
        long otherCandidate = drivingIterators[i].currentCandidate();
        candidate = (candidate <= otherCandidate) ? candidate : otherCandidate;
      }
    }
    return candidate;
  }

  @Override
  public boolean hasMatch(ScoringContext candidate) {
    for (BaseIterator iterator : drivingIterators) {
      if (iterator.hasMatch(candidate)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isDone() {
    for (BaseIterator iterator : drivingIterators) {
      if (!iterator.isDone()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void reset() throws IOException {
    for (BaseIterator iterator : iterators) {
      iterator.reset();
    }
  }

  @Override
  public boolean hasAllCandidates() {
    return hasAllCandidates;
  }

  @Override
  public long totalEntries() {
    long total = 0;
    for (BaseIterator i : this.iterators) {
      if (i.hasAllCandidates()) {
        return i.totalEntries();
      } else {
        total += i.totalEntries();
      }
    }
    return total;
  }
}
