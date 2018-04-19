package edu.umass.cics.ciir.irene

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import edu.umass.cics.ciir.irene.indexing.LDocBuilder
import edu.umass.cics.ciir.irene.lang.IreneQueryLanguage
import edu.umass.cics.ciir.irene.lang.MultiExpr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.TextExpr
import edu.umass.cics.ciir.irene.ltr.RREnv
import edu.umass.cics.ciir.irene.scoring.IreneQueryModel
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.benchmark.byTask.feeds.DocData
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException
import org.apache.lucene.benchmark.byTask.feeds.TrecContentSource
import org.apache.lucene.index.*
import org.apache.lucene.search.*
import org.lemurproject.galago.utility.Parameters
import java.io.Closeable
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinTask
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.experimental.buildSequence

/**
 *
 * @author jfoley.
 */

fun LDoc.toParameters(): Parameters {
    val output = Parameters.create()
    fields.forEach { field ->
        val name = field.name()!!
        output.putIfNotNull(name, field.stringValue())
        output.putIfNotNull(name, field.numericValue())
    }
    return output
}

class IndexParams {
    var defaultField = "body"
    var defaultAnalyzer: Analyzer = IreneEnglishAnalyzer()
    private var perFieldAnalyzers = HashMap<String, Analyzer>()
    var directory: RefCountedIO? = null
    var openMode: IndexWriterConfig.OpenMode? = null
    var idFieldName = "id"

    fun withAnalyzer(field: String, analyzer: Analyzer) {
        perFieldAnalyzers.put(field, analyzer)
    }
    fun inMemory() {
        directory = RefCountedIO(MemoryIO())
        create()
    }
    fun withPath(fp: File) {
        directory = RefCountedIO(DiskIO.open(fp.toPath()))
    }
    fun create() {
        openMode = IndexWriterConfig.OpenMode.CREATE
    }
    fun append() {
        openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND
    }
    val analyzer: Analyzer
            get() = if (perFieldAnalyzers.isEmpty()) {
                defaultAnalyzer
            } else {
                PerFieldAnalyzerWrapper(defaultAnalyzer, perFieldAnalyzers)
            }
}

class IreneIndexer(val params: IndexParams) : Closeable {
    companion object {
        fun build(setup: IndexParams.() -> Unit): IreneIndexer {
            return IreneIndexer(IndexParams().apply(setup))
        }
    }
    val processed = AtomicLong(0)
    val dest = params.directory!!
    val writer = IndexWriter(dest.use(), IndexWriterConfig(params.analyzer).apply {
        similarity = TrueLengthNorm()
        openMode = params.openMode
    })
    override fun close() {
        writer.close()
    }
    fun commit() {
        writer.commit()
    }
    fun push(vararg doc: IndexableField): Long {
        writer.addDocument(doc.toList())
        return processed.incrementAndGet()
    }
    fun push(doc: Iterable<IndexableField>): Long {
        writer.addDocument(doc)
        return processed.incrementAndGet()
    }
    fun open() = IreneIndex(dest, params)

    fun doc(fn: LDocBuilder.()->Unit): Long {
        val doc = LDocBuilder(params)
        fn(doc)
        return push(doc.finish())
    }
}

interface IIndex : Closeable {
    val tokenizer: GenericTokenizer
    val defaultField: String
    val totalDocuments: Int
    fun getRREnv(): RREnv
    fun fieldStats(field: String): CountStats?
    fun getStats(expr: QExpr): CountStats
    fun getStats(text: String, field: String = defaultField): CountStats = getStats(Term(field, text))
    fun getStats(term: Term): CountStats
    fun tokenize(text: String, field: String=defaultField) = tokenizer.tokenize(text, field)
    fun toTextExprs(text: String, field: String = defaultField): List<TextExpr> = tokenize(text, field).map { TextExpr(it, field) }
    fun search(q: QExpr, n: Int): TopDocs
    fun documentById(id: String): Int?
    fun explain(q: QExpr, doc: String): Explanation? {
        val internal = documentById(doc) ?: return null
        return explain(q, internal)
    }
    fun explain(q: QExpr, doc: Int): Explanation
    fun docAsParameters(doc: Int): Parameters?

    fun getLTRDoc(id: String, fields: Set<String>): LTRDoc? {
        val num = documentById(id) ?: return null
        val fjson = docAsParameters(num) ?: return null
        return LTRDoc.create(id, fjson, fields, defaultField, tokenizer)
    }
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
    override fun docAsParameters(doc: Int): Parameters? = null
}

class IreneIndex(val io: RefCountedIO, val params: IndexParams) : IIndex {
    constructor(params: IndexParams) : this(params.directory!!, params)
    val jobPool = ForkJoinPool.commonPool()
    val idFieldName = params.idFieldName
    val reader = DirectoryReader.open(io.open().use())
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
        val resp = idToNameCache.get(doc, {
            getField(doc, idFieldName)?.stringValue() ?: ""
        }) ?: return null
        if (resp.isBlank()) return null
        return resp
    }
    override fun docAsParameters(doc: Int): Parameters? {
        val ldoc = document(doc) ?: return null
        val fields = Parameters.create()
        ldoc.fields.forEach { field ->
            val name = field.name()!!
            fields.putIfNotNull(name, field.stringValue())
            fields.putIfNotNull(name, field.numericValue())
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
        val response = nameToIdCache.get(id, { missing -> documentByIdInternal(missing) ?: -1 })
        if (response == null || response < 0) return null
        return response
    }

    fun terms(doc: Int, field: String = defaultField): List<String> {
        val text = getField(doc, field)?.stringValue() ?: return emptyList()
        return tokenize(text, field)
    }

    fun getAverageDL(field: String): Double = fieldStats(field)?.avgDL() ?: error("No such field $field.")

    override fun fieldStats(field: String): CountStats? {
        return fieldStatsCache.computeIfAbsent(field, {
            CalculateStatistics.fieldStats(searcher, field)
        })
    }

    override fun getStats(term: Term): CountStats {
        //println("getStats($term)")
        return termStatsCache.get(term, {CalculateStatistics.lookupTermStatistics(searcher, it)})
                ?: fieldStats(term.field())
                ?: error("No such field ${term.field()}.")
    }
    override fun getStats(expr: QExpr): CountStats {
        if (expr is TextExpr) {
            return getStats(expr.text, expr.statsField())
        }
        return getExprStats(expr)!!.join()
    }

    fun prepare(expr: QExpr): IreneQueryModel = IreneQueryModel(this, this.env, expr)

    private fun getExprStats(expr: QExpr): ForkJoinTask<CountStats>? {
        return exprStatsCache.get(expr, { missing ->
            val func: ()->CountStats = {CalculateStatistics.computeQueryStats(searcher, prepare(missing), this::fieldStats)}
            jobPool.submit(func)
        })
    }

    override fun search(q: QExpr, n: Int): TopDocs {
        if (n == 0) return TopDocs(0L, emptyArray(), Float.NaN)
        return searcher.search(prepare(q), TopKCollectorManager(n))!!
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

fun TrecContentSource.docs(): Sequence<DocData> = buildSequence {
    while(true) {
        val doc = DocData()
        try {
            getNextDocData(doc)
            yield(doc)
        } catch (e: NoMoreDataException) {
            break
        } catch (e: Throwable) {
            e.printStackTrace(System.err)
        }
    }
}

