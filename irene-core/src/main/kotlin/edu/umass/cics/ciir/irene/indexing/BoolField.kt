package edu.umass.cics.ciir.irene.indexing

import org.apache.lucene.document.Field
import org.apache.lucene.document.FieldType
import org.apache.lucene.index.IndexOptions

/**
 *
 * @author jfoley.
 */

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


