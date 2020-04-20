package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.lang.CountStats
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.RREnv
import edu.umass.cics.ciir.irene.lang.TextExpr
import edu.umass.cics.ciir.irene.utils.ReservoirSampler
import org.apache.lucene.index.Term
import org.apache.lucene.search.Explanation
import org.apache.lucene.search.TopDocs
import org.roaringbitmap.RoaringBitmap
import java.io.Closeable
import java.util.*
import java.util.concurrent.ThreadLocalRandom

/**
 *
 * @author jfoley.
 */
interface IIndex : Closeable {
    val tokenizer: GenericTokenizer
    val defaultField: String
    val totalDocuments: Int
    fun getRREnv(): RREnv
    fun fieldStats(field: String): CountStats?
    fun getStats(expr: QExpr): CountStats
    fun getStats(text: String, field: String = defaultField): CountStats = getStats(Term(field, text))
    fun getStats(term: Term): CountStats
    fun tokenize(text: String) = tokenize(text, defaultField)
    fun tokenize(text: String, field: String) = tokenizer.tokenize(text, field)
    fun toTextExprs(text: String, field: String = defaultField): List<TextExpr> = tokenize(text, field).map { TextExpr(it, field) }
    fun search(q: QExpr, n: Int): TopDocs
    fun documentById(id: String): Int?
    fun explain(q: QExpr, doc: String): Explanation? {
        val internal = documentById(doc) ?: return null
        return explain(q, internal)
    }
    fun explain(q: QExpr, doc: Int): Explanation
    fun docAsMap(doc: Int): Map<String, Any>?

    //fun getLTRDoc(id: String, fields: Set<String>): LTRDoc? {
        //val num = documentById(id) ?: return null
        //val fjson = docAsParameters(num) ?: return null
        //return LTRDoc.create(id, fjson, fields, tokenizer)
    //}
    fun count(q: QExpr): Int
    fun matches(q: QExpr): RoaringBitmap
    fun sample(q: QExpr, n: Int, rand: Random): ReservoirSampler<Int>
    fun sample(q: QExpr, n: Int): ReservoirSampler<Int> {
        return this.sample(q, n, ThreadLocalRandom.current())
    }

    fun terms(doc: Int, field: String): List<String>
}

class EmptyIndex(override val tokenizer: GenericTokenizer = WhitespaceTokenizer()) : IIndex {
    override val defaultField: String = "missing"
    override val totalDocuments: Int = 0
    override fun fieldStats(field: String): CountStats? = null
    override fun getStats(expr: QExpr): CountStats = CountStats("EmptyIndex($expr)", expr.getSingleStatsField(defaultField))
    override fun getStats(term: Term): CountStats = CountStats("EmptyIndex($term)", term.field())
    override fun close() { }
    override fun search(q: QExpr, n: Int): TopDocs = TopDocs(0L, emptyArray(), -Float.MAX_VALUE)
    override fun getRREnv(): RREnv = error("No RREnv for EmptyIndex.")
    override fun documentById(id: String): Int? = null
    override fun explain(q: QExpr, doc: Int): Explanation {
        return Explanation.noMatch("EmptyIndex")
    }
    override fun terms(doc: Int, field: String): List<String> = error("EmptyIndex does not have field=$field for doc=$doc")
    override fun docAsMap(doc: Int): Map<String, Any>? = null
    override fun count(q: QExpr): Int = 0
    override fun matches(q: QExpr): RoaringBitmap = RoaringBitmap()
    override fun sample(q: QExpr, n: Int, rand: Random): ReservoirSampler<Int> = ReservoirSampler(n, rand)
}
