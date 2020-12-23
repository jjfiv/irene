package edu.umass.cics.ciir.irene

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import edu.umass.cics.ciir.irene.index.IIndex
import edu.umass.cics.ciir.irene.indexing.IndexParams
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.lucene.*
import edu.umass.cics.ciir.irene.scoring.IreneQueryModel
import edu.umass.cics.ciir.irene.utils.ReservoirSampler
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexableField
import org.apache.lucene.index.Term
import org.apache.lucene.search.*
import org.roaringbitmap.RoaringBitmap
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinTask

/**
 *
 * @author jfoley.
 */
class IreneIndex(val io: RefCountedIO, val params: IndexParams) : IIndex {
    constructor(params: IndexParams) : this(params.directory!!, params)
    val jobPool: ForkJoinPool = ForkJoinPool.commonPool()
    val idFieldName = params.idFieldName
    val reader: DirectoryReader = DirectoryReader.open(io.open().use())
    val searcher = IndexSearcher(reader, jobPool)
    val analyzer = params.analyzer
    val env = IreneQueryLanguage(this)
    override val tokenizer: LuceneTokenizer = LuceneTokenizer(analyzer)
    override val defaultField: String get() = params.defaultField
    override val totalDocuments: Int get() = reader.numDocs()
    override fun getRREnv(): RREnv = env

    private val termStatsCache: Cache<Term, CountStats> = Caffeine.newBuilder().maximumSize(100_000).build()
    private val exprStatsCache = Caffeine.newBuilder().maximumSize(100_000).build<QExpr, ForkJoinTask<CountStats>>()
    private val fieldStatsCache = ConcurrentHashMap<String, CountStats?>()
    private val nameToIdCache: Cache<String, Int> = Caffeine.newBuilder().maximumSize(100_000).build()
    private val idToNameCache: Cache<Int, String> = Caffeine.newBuilder().maximumSize(100_000).build()

    override fun close() {
        reader.close()
        io.close()
    }

    fun getField(doc: Int, name: String): IndexableField? = searcher.doc(doc, setOf(name))?.getField(name)
    fun getDocumentName(doc: Int): String? {
        val resp = idToNameCache.get(doc) {
            getField(doc, idFieldName)?.stringValue() ?: ""
        } ?: return null
        if (resp.isBlank()) return null
        return resp
    }
    override fun docAsMap(doc: Int): Map<String, Any>? {
        val ldoc = document(doc) ?: return null
        val fields = hashMapOf<String, Any>()
        ldoc.fields.forEach { field ->
            val name = field.name()!!
            field.stringValue()?.let {
                fields.put(name, it)
            }
            field.numericValue()?.let {
                fields.put(name, it)
            }
        }
        return fields
    }
    fun document(doc: Int): LDoc? {
        return lucene_try { searcher.doc(doc) }
    }
    fun document(doc: Int, fields: Set<String>): LDoc? {
        return lucene_try { searcher.doc(doc, fields) }
    }
    private fun documentByIdInternal(id: String): Int? {
        val q = BooleanQuery.Builder().add(TermQuery(Term(idFieldName, id)), BooleanClause.Occur.MUST).build()!!
        return lucene_try {
            val results = searcher.search(q, 10)?.scoreDocs
            if (results == null || results.isEmpty()) return null
            // TODO complain about dupes?
            return results[0].doc
        }
    }
    override fun documentById(id: String): Int? {
        val response = nameToIdCache.get(id) { missing -> documentByIdInternal(missing) ?: -1 }
        if (response == null || response < 0) return null
        return response
    }

    override fun terms(doc: Int, field: String): List<String> {
        val text = getField(doc, field)?.stringValue() ?: return emptyList()
        return tokenize(text, field)
    }

    fun getAverageDL(field: String): Double = fieldStats(field)?.avgDL() ?: error("No such field $field.")

    override fun fieldStats(field: String): CountStats? {
        return fieldStatsCache.computeIfAbsent(field) {
            CalculateStatistics.fieldStats(searcher, field)
        }
    }

    override fun getStats(term: Term): CountStats {
        //println("getStats($term)")
        return termStatsCache.get(term, { CalculateStatistics.lookupTermStatistics(searcher, it)})
                ?: fieldStats(term.field())
                ?: error("No such field ${term.field()}.")
    }
    override fun getStats(expr: QExpr): CountStats {
        expr.applyEnvironment(this.env)
        if (expr is TextExpr) {
            return getStats(expr.text, expr.statsField())
        }
        return getExprStats(expr)!!.join()
    }

    fun prepare(expr: QExpr): IreneQueryModel = IreneQueryModel(this, this.env, expr)

    private fun getExprStats(expr: QExpr): ForkJoinTask<CountStats>? {
        return exprStatsCache.get(expr) { missing ->
            val func: ()-> CountStats = { CalculateStatistics.computeQueryStats(searcher, prepare(missing), this::fieldStats)}
            jobPool.submit(func)
        }
    }

    override fun search(q: QExpr, n: Int): TopDocs {
        if (n == 0) return TopDocs(0L, emptyArray(), Float.NaN)
        return searcher.search(prepare(q), TopKCollectorManager(n))!!
    }
    override fun count(q: QExpr): Int {
        val lq = prepare(q)
        return searcher.count(lq)
    }
    override fun matches(q: QExpr): RoaringBitmap {
        return searcher.search(prepare(q), BitmapCollectorManager())
    }

    override fun sample(q: QExpr, n: Int, rand: Random): ReservoirSampler<Int> {
        return searcher.search(prepare(q), SamplingCollectorManager(n, rand))
    }

    fun pool(qs: Map<String, QExpr>, depth: Int): Map<String, TopDocs> {
        val multiExpr = MultiExpr(qs)
        return searcher.search(prepare(multiExpr), PoolingCollectorManager(multiExpr, depth))
    }
    override fun explain(q: QExpr, doc: Int): Explanation = searcher.explain(prepare(q), doc)
}

fun toSimpleString(input: QExpr, out: Appendable, prefix: String="") {
    input.visitWithDepth({ q, depth ->
        out.append(prefix)
        (0 until depth).forEach { out.append("  ") }
        out.append(q.javaClass.simpleName)
        if (q is TextExpr) {
            out.append('\t').append(q.text)
        }
        out.append("\n")
    })
}
