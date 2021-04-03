package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * @author jfoley.
 */
public class UnorderedWindowBigramIterator extends ExtentConjunctionIterator {

    int width;
    private ScoringContext cachedContext = null;

    /**
     * Creates a new create of UnorderedWindowIterator
     */
    public UnorderedWindowBigramIterator(NodeParameters parameters, ExtentIterator[] evIterators) throws IOException {
        super(parameters, evIterators);
        this.width = (int) parameters.get("default", -1);
        assert (evIterators.length == 2) : "UnorderedWindowBigramIterator requires exactly two arguments.";
        syncTo(0);
    }

    @Override
    public void loadExtentsCommon(ScoringContext c) {

        // we can not load any extentCache if the iterator is done - or is at the wrong document.
        if (iterators[0].isDone() || iterators[1].isDone() || !iterators[0].hasMatch(c) || !iterators[1].hasMatch(c)) {
            return;
        }

        ExtentArrayIterator iterA = new ExtentArrayIterator(((ExtentIterator) iterators[0]).extents(c));
        ExtentArrayIterator iterB = new ExtentArrayIterator(((ExtentIterator) iterators[1]).extents(c));

        if (iterA.isDone() || iterB.isDone()) {
            return;
        }

        final boolean anySizeWindow = (width < 0);

        boolean hasNext = true;
        while (hasNext) {
            // choose minimum iterator based on start
            final ExtentArrayIterator minIter = (iterA.currentBegin() < iterB.currentBegin()) ? iterA : iterB;
            final int minimumPosition = minIter.currentBegin();
            final int maximumPosition = Math.max(iterA.currentEnd(), iterB.currentEnd());

            // check for a match
            if (anySizeWindow || maximumPosition - minimumPosition <= width) {
                extentCache.add(minimumPosition, maximumPosition);
            }

            // move minimum iterator
            hasNext = minIter.next();
        }
    }

}
