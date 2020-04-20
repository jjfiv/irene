package edu.umass.cics.ciir.irene.indexing

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.shingle.ShingleFilter
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute
import org.apache.lucene.document.FieldType
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.index.IndexableField
import org.apache.lucene.index.IndexableFieldType
import org.apache.lucene.util.BytesRef
import java.io.Reader

/**
 * @author jfoley
 */
class ListTokenStream(val items: List<String>) : TokenStream() {
    val length = items.size

    val term = addAttribute(CharTermAttribute::class.java)
    val offset = addAttribute(OffsetAttribute::class.java)
    /** Keep track of our offset into a "text" version of this field **/
    var pos = 0
    var charOffset = 0

    override fun reset() {
        pos = 0
        charOffset = 0
    }

    override fun incrementToken(): Boolean {
        if (pos < length) {
            val text = items[pos]
            term.setLength(0)
            term.append(text)
            offset.setOffset(charOffset, charOffset+text.length)
            // Step by text and 1 space.
            charOffset += text.length + 1
            pos++
            return true
        }
        return false
    }
}


class AlreadyTokenizedEfficientCountField(
        val field: String,
        val text: String,
        val terms: List<String>,
        val stored: Boolean,
        val n: Int = 1
) : IndexableField {
    override fun name(): String = field
    override fun stringValue(): String? = if (stored) { text } else null
    override fun numericValue(): Number? = null
    override fun binaryValue(): BytesRef? = null
    override fun readerValue(): Reader? = null
    override fun fieldType(): IndexableFieldType = if (stored) {
        storedType
    } else {
        notStoredType
    }
    override fun tokenStream(analyzer: Analyzer?, reuse: TokenStream?): TokenStream {
        val ts = ListTokenStream(terms)
        if (n == 1) {
            return ts
        } else if (n == 2) {
            // output bigrams as well:
            val sf = ShingleFilter(ts, 2)
            sf.setOutputUnigrams(false)
            return sf
        } else error("Don't handle n=$n")
    }

    companion object {
        val notStoredType = FieldType().apply {
            setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            setTokenized(true);
            freeze();
        }
        val storedType = FieldType().apply {
            setIndexOptions(IndexOptions.DOCS_AND_FREQS);
            setTokenized(true);
            setStored(true);
            freeze();
        }
    }
}

class AlreadyTokenizedTextField(
        val field: String,
        val text: String,
        val terms: List<String>,
        val stored: Boolean
) : IndexableField {
    override fun name(): String = field
    override fun stringValue(): String? = if (stored) { text } else null
    override fun numericValue(): Number? = null
    override fun binaryValue(): BytesRef? = null
    override fun readerValue(): Reader? = null
    override fun fieldType(): IndexableFieldType = if (stored) {
        storedType
    } else {
        notStoredType
    }
    override fun tokenStream(analyzer: Analyzer?, reuse: TokenStream?): TokenStream = ListTokenStream(terms)

    companion object {
        val notStoredType = FieldType().apply {
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            setTokenized(true);
            freeze();
        }
        val storedType = FieldType().apply {
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            setTokenized(true);
            setStored(true);
            freeze();
        }
    }
}
