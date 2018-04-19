package edu.umass.cics.ciir.irene

import org.apache.lucene.document.Field
import org.apache.lucene.document.FieldType
import org.apache.lucene.index.FieldInvertState
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.CollectionStatistics
import org.apache.lucene.search.TermStatistics
import org.apache.lucene.search.similarities.Similarity

/**
 *
 * @author jfoley.
 */

fun StoreConstant(x: Boolean): Field.Store = if (x) Field.Store.YES else Field.Store.NO

class BoolField(name: String, x: Boolean, store: Boolean) : Field(name, boolAsStr(x), if (store) STORED_TYPE else NOT_STORED_TYPE) {
    companion object {
        val STORED_TYPE = FieldType().apply {
            setOmitNorms(true)
            setStored(true)
            setIndexOptions(IndexOptions.DOCS)
            setTokenized(false)
            freeze()
        }
        val NOT_STORED_TYPE = FieldType().apply {
            setOmitNorms(true)
            setStored(false)
            setIndexOptions(IndexOptions.DOCS)
            setTokenized(false)
            freeze()
        }
        const val TRUE = "T"
        const val FALSE = "F"
        fun boolAsStr(x: Boolean) = if(x) TRUE else FALSE
    }
}


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
