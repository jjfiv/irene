package edu.umass.cics.ciir.irene.indexing

import edu.umass.cics.ciir.irene.LDoc
import edu.umass.cics.ciir.irene.tokenize
import org.apache.lucene.document.*
import org.apache.lucene.index.IndexableField

/**
 *
 * @author jfoley.
 */
class LDocBuilder(val params: IndexParams) {
    val YES = Field.Store.YES
    val NO = Field.Store.NO
    val fields = HashMap<String, List<IndexableField>>()
    val analyzer = params.analyzer

    private fun storeBoolean(x: Boolean) = if(x) YES else NO

    fun setId(id: String) {
        if (fields.containsKey(params.idFieldName)) {
            error("Already specified ${params.idFieldName}=$id for this document, previous=${fields[params.idFieldName]!![0].stringValue()}.")
        }
        fields[params.idFieldName] = listOf(StringField(params.idFieldName, id, YES))
    }

    /**
     * This "setter" creates derived lucene fields from the same text.
     *  - A [AlreadyTokenizedTextField] of name [field] that contains a stream over the tokens using the correct analyzer.
     *  - A [NumericDocValuesField] of name "lengths:$[field]" which contains the true length; this lets Lucene use norms as it wishes.
     */
    fun setTextField(field: String, text: String, stored: Boolean=true) = setTextField(field, text, analyzer.tokenize(field, text), stored)

    fun maybeTextField(field: String, text: String?, stored: Boolean=true) {
        if (text != null && !text.isBlank()) {
            setTextField(field, text, stored)
        }
    }

    /**
     * This "setter" creates derived lucene fields from the same text.
     *  - A [AlreadyTokenizedTextField] of name [field] that contains a stream over the tokens using the correct analyzer.
     *  - A [NumericDocValuesField] of name "lengths:$[field]" which contains the true length; this lets Lucene use norms as it wishes.
     *  - Unless you need the list of terms yourself, prefer the version that does not request it, this will invoke the analyzer for you.
     */
    fun setTextField(field: String, text: String, terms: List<String>, stored: Boolean) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        val length = terms.size
        val uniqLength = terms.toSet().size

        val keep = ArrayList<IndexableField>()

        keep.add(AlreadyTokenizedTextField(field, text, terms, stored))
        keep.add(NumericDocValuesField("lengths:$field", length.toLong()))
        keep.add(NumericDocValuesField("unique:$field", uniqLength.toLong()))
        fields[field] = keep
    }

    /**
     * This is still half-baked (19 April 2018)
     * @author jfoley
     */
    fun setEfficientTextField(field: String, text: String, stored: Boolean=true) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        val terms = analyzer.tokenize(field, text)
        val length = terms.size
        val uniqLength = terms.toSet().size

        val keep = ArrayList<IndexableField>()
        keep.add(AlreadyTokenizedEfficientCountField(field, text, terms, stored, 1))
        // make this a separate field so that it does not affect count statistics.
        keep.add(AlreadyTokenizedEfficientCountField("od:$field", text, terms, stored, 2))
        keep.add(NumericDocValuesField("lengths:$field", length.toLong()))
        keep.add(NumericDocValuesField("lengths:od:$field", length.toLong()))
        keep.add(NumericDocValuesField("unique:$field", uniqLength.toLong()))
        fields[field] = keep
    }

    fun finish(): LDoc {
        if (!fields.containsKey(params.idFieldName)) {
            error("Generated document without an identifier field: ${params.idFieldName}!")
        }
        val ldoc = LDoc()
        fields.values.flatten().forEach { f -> ldoc.add(f) }
        //println(ldoc.fields.map { it.name() })
        return ldoc
    }

    fun setBoolField(field: String, value: Boolean, stored: Boolean = true) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        fields[field] = listOf(BoolField(field, value, stored))
    }

    fun setStringField(field: String, categorical: String, stored: Boolean=true) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        fields[field] = listOf(StringField(field, categorical, storeBoolean(stored)))
    }
    fun maybeIntPoint(field: String, num: Int?, stored: Boolean=true) {
        if (num == null) return;
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        val keep  = arrayListOf<IndexableField>(
                IntPoint(field, num)
        )
        if (stored) {
            keep.add(StoredField(field, num))
        }
        fields[field] = keep
    }
    fun setDenseFloatField(field: String, num: Float, stored: Boolean=true) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        val keep  = arrayListOf<IndexableField>(
                NumericDocValuesField(field, num.toRawBits().toLong())
        )
        if (stored) {
            keep.add(StoredField(field, num))
        }
        fields[field] = keep
    }
    fun setDenseIntField(field: String, num: Int, stored: Boolean=true) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        val keep  = arrayListOf<IndexableField>(
                NumericDocValuesField(field, num.toLong())
        )
        if (stored) {
            keep.add(StoredField(field, num))
        }
        fields[field] = keep
    }
    fun setDenseLongField(field: String, num: Long, stored: Boolean=true) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        val keep  = arrayListOf<IndexableField>(
                NumericDocValuesField(field, num)
        )
        if (stored) {
            keep.add(StoredField(field, num))
        }
        fields[field] = keep
    }
    fun setStoredString(field: String, data: String) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        fields[field] = listOf(StoredField(field, data))
    }

}
