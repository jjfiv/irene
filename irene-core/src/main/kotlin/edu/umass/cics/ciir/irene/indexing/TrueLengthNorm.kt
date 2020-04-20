package edu.umass.cics.ciir.irene.indexing

import org.apache.lucene.index.FieldInvertState
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.CollectionStatistics
import org.apache.lucene.search.TermStatistics
import org.apache.lucene.search.similarities.Similarity

/**
 *
 * @author jfoley.
 */
class TrueLengthNorm : Similarity() {
    override fun computeNorm(state: FieldInvertState?): Long {
        return state!!.length.toLong()
    }

    override fun simScorer(p0: SimWeight?, p1: LeafReaderContext?): SimScorer {
        throw UnsupportedOperationException()
    }

    override fun computeWeight(p0: Float, p1: CollectionStatistics?, vararg p2: TermStatistics?): SimWeight {
        throw UnsupportedOperationException()
    }
}
