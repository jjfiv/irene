package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.DataNeeded
import edu.umass.cics.ciir.irene.LuceneQuery
import edu.umass.cics.ciir.irene.indexing.BoolField
import edu.umass.cics.ciir.irene.lucene_try
import org.apache.lucene.index.Term
import org.apache.lucene.search.*


/**
 * [QExpr] is the base class for our typed inquery-like query language. Nodes  have [children], and can be [visit]ed, but they are also strongly typed, and can [deepCopy] themselves.
 *
 * A reasonable query may be composed of [TextExpr] at the leaves, [BM25Expr] in the middle and a [CombineExpr] across the terms, but much more sophisticated scoring models can be expressed.
 *
 * @author jfoley.
 */
sealed class QExpr {
    val trySingleChild: QExpr
        get() {
            if (children.size != 1) error("Looked for a child on a node with children: $this")
            return children[0]
        }
    abstract val children: List<QExpr>
    fun deepCopy(): QExpr = map {
        if (it is LeafExpr) {
            it.copyLeaf()
        } else it
    }
    open fun applyEnvironment(env: RREnv) {}

    fun requiresPRF(): Boolean {
        var requiresPRF = false
        this.visit {q ->
            when(q) {
                is RM3Expr -> {
                    requiresPRF = true
                    return@visit
                }
                else -> {}
            }
        }
        return requiresPRF
    }

    private fun findLuceneTextNodes(lq: Query, out: HashSet<TextExpr>) {
        when(lq) {
            is TermQuery -> out.add(TextExpr(lq.term))
            is BooleanQuery -> lq.clauses().map { clause -> findLuceneTextNodes(clause.query, out) }
            is PhraseQuery -> out.addAll(lq.terms.map { TextExpr(it) })
            is ConstantScoreQuery -> findLuceneTextNodes(lq.query, out)
            is DisjunctionMaxQuery -> lq.disjuncts.forEach { qc ->
                findLuceneTextNodes(qc, out)
            }

            // TODO(someday) wildcards need to be expanded first.
            is WildcardQuery -> out.add(TextExpr(lq.term))
            is FuzzyQuery -> out.add(TextExpr(lq.term))
            is RegexpQuery -> out.add(TextExpr(lq.regexp))
            is PrefixQuery -> out.add(TextExpr(lq.prefix))
            is MultiPhraseQuery -> out.addAll(lq.termArrays.flatMap { arr -> arr.map{ TextExpr(it) } })

            is PointRangeQuery,
            is TermRangeQuery -> TODO("Can't support this now...")


            is MatchAllDocsQuery -> {} // TODO, represent this?
        }
    }
    fun findTextNodes(): List<TextExpr> {
        val out = HashSet<TextExpr>()
        visit {
            if (it is TextExpr) {
                out.add(it)
            } else if (it is LuceneExpr) {
                findLuceneTextNodes(it.query!!, out)
            }
        }
        return out.sorted()
    }

    fun visit(each: (QExpr)->Unit) {
        each(this)
        children.forEach { it.visit(each) }
    }

    fun visitWithDepth(each: (QExpr, Int)->Unit, depth: Int = 0) {
        each(this, depth)
        children.forEach { it.visitWithDepth(each, depth+1) }
    }

    fun getStatsFields(): Set<String> {
        val out = HashSet<String>()
        visit { c ->
            if (c is TextExpr) {
                out.add(c.statsField())
            }
        }
        return out
    }
    fun getSingleStatsField(default: String): String {
        val fields = getStatsFields()
        return when(fields.size) {
            0 -> default
            1 -> fields.first()
            else -> error("Can't determine single field for $this")
        }
    }
    fun getLengthsFields(): Set<String> {
        val out = HashSet<String>()
        visit { c ->
            if (c is TextExpr) {
                out.add(c.countsField())
            }
        }
        return out
    }
    fun getLengthsField(): String {
        val fields = getLengthsFields()
        return when(fields.size) {
            0 -> error("No lengths field found for $this.")
            1 -> fields.first()
            else -> error("Can't determine single field for $this")
        }
    }

    fun map(mapper: (QExpr)->QExpr): QExpr = qmap(this, mapper)

    // Get a weighted version of this node if weight is non-null.
    fun weighted(x: Double?) = if(x != null) WeightExpr(this, x) else this

    // Defines a mixture model of [lambda] times the current expression and 1.0-[lambda] times the [rhs] expression.
    fun mixed(lambda: Double, rhs: QExpr) = SumExpr(this.weighted(lambda), rhs.weighted(1.0 -lambda))
}
data class MultiExpr(val namedExprs: Map<String, QExpr>): QExpr() {
    val names = namedExprs.keys.toList()
    override val children = names.map { namedExprs[it]!! }.toList()

}
sealed class LeafExpr : QExpr() {
    override val children: List<QExpr> get() = emptyList()
    abstract fun copyLeaf(): QExpr
}

/**
 * We will never score a document just because of any constant.
 * If you want that behavior, check out [AlwaysMatchExpr] that you can wrap these with.
 */
sealed class ConstExpr() : LeafExpr() { }
data class ConstScoreExpr(var x: Double): ConstExpr() {
    override fun copyLeaf(): QExpr = ConstScoreExpr(x)
}
data class ConstCountExpr(var x: Int): ConstExpr() {
    override fun copyLeaf(): QExpr = ConstCountExpr(x)
}
data class ConstBoolExpr(var x: Boolean): ConstExpr() {
    override fun copyLeaf(): QExpr = ConstBoolExpr(x)
}
/**
 * For finding document candidates, consider this subtree to *always* cause a match.
 * Hope you put in an And (e.g., [AndExpr] or [OrderedWindowExpr]), or this will be extremely expensive.
 */
fun AlwaysMatchExpr(child: QExpr) = RequireExpr(AlwaysMatchLeaf, child)
object AlwaysMatchLeaf : LeafExpr() {
    override fun copyLeaf() = this
}

/**
 * For finding document candidates, never consider this subtree as a match. This is the opposite of [AlwaysMatchExpr].
 * Useful for "boost" style features that are expensive.
 * Don't use this much, we should be able to infer it in many cases, see [createOptimizedMovementExpr] and [simplifyBooleanExpr].
 */
fun NeverMatchExpr(child: QExpr) = RequireExpr(NeverMatchLeaf, child)
object NeverMatchLeaf : LeafExpr() {
    override fun copyLeaf() = this
}

data class WhitelistMatchExpr(var docNames: Set<String>? = null, var docIdentifiers: List<Int>? = null) : LeafExpr() {
    override fun applyEnvironment(env: RREnv) {
        if (docIdentifiers == null) {
            if (docNames == null) error("WhitelistMatchExpr must have *either* docNames or docIdentifiers to start.")
            docIdentifiers = env.lookupNames(docNames!!)
        }
    }
    override fun copyLeaf() = WhitelistMatchExpr(docNames, docIdentifiers)
}

data class DenseLongField(val name: String, var missing: Long=0L): LeafExpr() {
    override fun copyLeaf() = DenseLongField(name, missing)
}
data class DenseFloatField(val name: String, var missing: Float=-Float.MAX_VALUE): LeafExpr() {
    override fun copyLeaf() = DenseFloatField(name, missing)
}
/** Match if a long expression is less than or equal to a threshold. (e.g., [DenseLongField])  */
data class LongLTE(override var child: QExpr, val threshold: Long): SingleChildExpr() { }

data class LengthsExpr(var statsField: String?) : LeafExpr() {
    override fun applyEnvironment(env: RREnv) {
        if (statsField == null) {
            statsField = env.defaultField
        }
    }
    override fun copyLeaf() = LengthsExpr(statsField)
}
sealed class OpExpr : QExpr() {
    abstract override var children: List<QExpr>
}
sealed class SingleChildExpr : QExpr() {
    abstract var child: QExpr
    override val children: List<QExpr> get() = listOf(child)
}
/**
 * Sync this class to Galago semantics.
 *  - Consider every doc that has a match IFF cond has a match, using value, regardless of whether value also has a match.
 *  - Implemented by [RequireEval].
 *  - If instead you want to score value only if cond has a match, use [MustExpr] -> [MustEval].
 */
data class RequireExpr(var cond: QExpr, var value: QExpr): QExpr() {
    override val children: List<QExpr> get() = arrayListOf(cond, value)
}

/**
 * Score the [value] query when it matches IFF [must] also has a match. This is a logical AND.
 */
data class MustExpr(var must: QExpr, var value: QExpr): QExpr() {
    override val children: List<QExpr> get() = arrayListOf(must, value)
}

fun BoolExpr(field: String, desired: Boolean=true): QExpr {
    return TextExpr(BoolField.boolAsStr(desired), field=field, statsField=field)
}

/**
 * [TextExpr] represent a term [text] inside a [field] smoothed with statistics [stats] derived from [statsField]. By default [field] and [statsField] will be the same, and will be filled with sane defaults if left empty.
 */
data class TextExpr(var text: String, var field: String? = null, var statsField: String? = null, var needed: DataNeeded = DataNeeded.DOCS) : LeafExpr(), Comparable<TextExpr> {
    override fun compareTo(other: TextExpr): Int {
        val cmp = (field ?: "").compareTo(other.field ?: "")
        if (cmp == 0) {
            return text.compareTo(other.text)
        }
        return cmp
    }

    override fun copyLeaf() = TextExpr(text, field, statsField, needed)
    constructor(term: Term) : this(term.text(), term.field())

    override fun applyEnvironment(env: RREnv) {
        if (field == null) {
            field = env.defaultField
        }
        if (statsField == null) {
            statsField = env.defaultField
        }
    }
    fun getStats(env: RREnv) = env.getStats(text, statsField())
    fun countsField(): String = field ?: error("No primary field for $this")
    fun statsField(): String = statsField ?: field ?: error("No stats field for $this")
}
data class SynonymExpr(override var children: List<QExpr>): OpExpr() {
}
data class LuceneExpr(val rawQuery: String, var query: LuceneQuery? = null ) : LeafExpr() {
    fun parse(env: IreneQueryLanguage) {
        if (this.query != null) {
            return
        }
        this.query = lucene_try {
            env.luceneQueryParser.parse(rawQuery)
        } ?: error("Could not parse lucene expression: ``${rawQuery}''")
    }
    override fun applyEnvironment(env: RREnv) {
        if (env is IreneQueryLanguage) {
            this.parse(env)
        } else {
            error("Cannot have LuceneExpr in environment=$env")
        }
    }
    override fun copyLeaf() = LuceneExpr(rawQuery, query)
}


data class AndExpr(override var children: List<QExpr>) : OpExpr() {
}
data class OrExpr(override var children: List<QExpr>) : OpExpr() {
}

fun SumExpr(vararg children: QExpr) = SumExpr(children.toList())
fun SumExpr(children: List<QExpr>) = CombineExpr(children, children.map { 1.0 })
fun MeanExpr(vararg children: QExpr) = MeanExpr(children.toList())
fun MeanExpr(children: List<QExpr>) = CombineExpr(children, children.map { 1.0 / children.size.toDouble() })
data class CombineExpr(override var children: List<QExpr>, var weights: List<Double>) : OpExpr() {
    val entries: List<Pair<QExpr, Double>> get() = children.zip(weights)
}
data class MultExpr(override var children: List<QExpr>) : OpExpr() {
}
data class MaxExpr(override var children: List<QExpr>) : OpExpr() {
}

/** For estimating the lower-bound of an [OrderedWindowExpr]. When all terms occur, which is smallest? */
data class SmallerCountExpr(override var children: List<QExpr>): OpExpr() {
    init {
        assert(children.size >= 2)
    }
}
/** For estimating the ceiling of an [UnorderedWindowExpr]. When all terms occur, which is biggest? */
data class UnorderedWindowCeilingExpr(override var children: List<QExpr>, var width: Int=8): OpExpr() {
}
data class OrderedWindowExpr(override var children: List<QExpr>, var step: Int=1) : OpExpr() {
}

/**
 * This [UnorderedWindowExpr] matches the computation in Galago. Huston et al. found that the particular unordered window does not matter so much, so we recommend using [ProxExpr] instead. [Tech Report](http://ciir-publications.cs.umass.edu/pub/web/getpdf.php?id=1142).
 */
data class UnorderedWindowExpr(override var children: List<QExpr>, var width: Int=8) : OpExpr() {
}

/** [ProxWindow] */
data class ProxExpr(override var children: List<QExpr>, var width: Int=8): OpExpr() {
}

data class WeightExpr(override var child: QExpr, var weight: Double = 1.0) : SingleChildExpr() {
}

data class CountEqualsExpr(override var child: QExpr, var target: Int): SingleChildExpr() {
}

data class LogValueExpr(override var child: QExpr): SingleChildExpr() {
}

data class DirQLExpr(override var child: QExpr, var mu: Double? = null, var stats: CountStats? = null): SingleChildExpr() {
    override fun applyEnvironment(env: RREnv) {
        this.mu = env.config.defaultDirichletMu
    }
}
data class LinearQLExpr(override var child: QExpr, var lambda: Double? = null, var stats: CountStats? = null): SingleChildExpr() {
    override fun applyEnvironment(env: RREnv) {
        this.lambda = env.config.defaultLinearSmoothingLambda
    }
}
data class AbsoluteDiscountingQLExpr(override var child: QExpr, var delta: Double? = null, var stats: CountStats? = null): SingleChildExpr() {
    override fun applyEnvironment(env: RREnv) {
        this.delta = env.config.absoluteDiscountingDelta
    }
}
data class BM25Expr(override var child: QExpr, var b: Double? = null, var k: Double? = null, var stats: CountStats? = null, var extractedIDF: Boolean = false): SingleChildExpr() {
    override fun applyEnvironment(env: RREnv) {
        if (b == null) b = env.config.defaultBM25b
        if (k == null) k = env.config.defaultBM25k
    }
}
data class CountToScoreExpr(override var child: QExpr): SingleChildExpr() {
}
data class BoolToScoreExpr(override var child: QExpr, var trueScore: Double=1.0, var falseScore: Double=0.0): SingleChildExpr() {
}
data class CountToBoolExpr(override var child: QExpr, var gt: Int = 0): SingleChildExpr() {
}

/** This is a two-pass query node. */
data class RM3Expr(override var child: QExpr, var origWeight: Double=0.3,
                   var fbDocs: Int=10, var fbTerms: Int=100,
                   var field: String? = null, var stopwords: Boolean = true): SingleChildExpr() {
}

/**
 * Apply a mapping function ([mapper]) to all nodes in this query ([q]), building a new query with the output nodes ([QExpr]).
 */
fun qmap(q: QExpr, mapper: (QExpr)->QExpr): QExpr {
    return when (q) {
        // LeafExpr:
        is LengthsExpr,
        is TextExpr,
        is LuceneExpr,
        is WhitelistMatchExpr,
        AlwaysMatchLeaf,
        NeverMatchLeaf,
        is ConstBoolExpr,
        is ConstScoreExpr,
        is DenseLongField,
        is DenseFloatField,
        is ConstCountExpr -> mapper(q)

        is MultiExpr -> mapper(MultiExpr(q.namedExprs.mapValues { (_, v) -> qmap(v, mapper) }))
        is SynonymExpr -> mapper(SynonymExpr(q.children.map { qmap(it, mapper) }))
        is AndExpr -> mapper(AndExpr(q.children.map { qmap(it, mapper) }))
        is OrExpr -> mapper(OrExpr(q.children.map { qmap(it, mapper) }))
        is CombineExpr -> mapper(CombineExpr(q.children.map { qmap(it, mapper) }, q.weights.toList()))
        is MultExpr -> mapper(MultExpr(q.children.map { qmap(it, mapper) }))
        is MaxExpr -> mapper(MaxExpr(q.children.map { qmap(it, mapper) }))
        is SmallerCountExpr -> mapper(SmallerCountExpr(q.children.map { qmap(it, mapper) }))
        is UnorderedWindowCeilingExpr -> mapper(UnorderedWindowCeilingExpr(q.children.map { qmap(it, mapper) }, q.width))
        is OrderedWindowExpr -> mapper(OrderedWindowExpr(q.children.map { qmap(it, mapper) }, q.step))
        is UnorderedWindowExpr -> mapper(UnorderedWindowExpr(q.children.map { qmap(it, mapper) }, q.width))
        is ProxExpr -> mapper(ProxExpr(q.children.map { qmap(it, mapper) }, q.width))
        is LogValueExpr -> mapper(LogValueExpr(qmap(q.child, mapper)))
        is WeightExpr -> mapper(WeightExpr(qmap(q.child, mapper), q.weight))
        is CountEqualsExpr -> mapper(CountEqualsExpr(qmap(q.child, mapper), q.target))
        is DirQLExpr -> mapper(DirQLExpr(qmap(q.child, mapper), q.mu, q.stats))
        is LinearQLExpr -> mapper(LinearQLExpr(qmap(q.child, mapper), q.lambda, q.stats))
        is AbsoluteDiscountingQLExpr -> mapper(AbsoluteDiscountingQLExpr(qmap(q.child, mapper), q.delta, q.stats))
        is BM25Expr -> mapper(BM25Expr(qmap(q.child, mapper), q.b, q.k, q.stats, q.extractedIDF))
        is CountToScoreExpr -> mapper(CountToScoreExpr(qmap(q.child, mapper)))
        is BoolToScoreExpr -> mapper(BoolToScoreExpr(qmap(q.child, mapper)))
        is CountToBoolExpr -> mapper(CountToBoolExpr(qmap(q.child, mapper)))
        is RequireExpr -> mapper(RequireExpr(qmap(q.cond, mapper), qmap(q.value, mapper)))
        is MustExpr -> mapper(MustExpr(qmap(q.must, mapper), qmap(q.value, mapper)))
        is LongLTE -> mapper(LongLTE(qmap(q.child, mapper), q.threshold))
        is RM3Expr -> mapper(RM3Expr(qmap(q.child, mapper), q.origWeight, q.fbDocs, q.fbTerms, q.field, q.stopwords))
    }
}
