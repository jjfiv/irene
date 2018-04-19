package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.createOptimizedMovementExpr
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.utils.Fraction
import gnu.trove.set.hash.TIntHashSet
import org.apache.lucene.search.DocIdSetIterator
import org.apache.lucene.search.Explanation

open class ScoringEnv(var doc: Int=-1) {
    open val ltr: LTRDoc get() = error("No LTR document available.")
}

const val NO_MORE_DOCS = DocIdSetIterator.NO_MORE_DOCS

/**
 * Try not to implement this directly, use one of [RecursiveEval], if you expect to have children [SingleChildEval] if you have a single child, or [LeafEvalNode] if you expect to have no children.
 */
interface QueryEvalNode {
    val children: List<QueryEvalNode>
    // Return a score for a document.
    fun score(env: ScoringEnv): Double
    // Return an count for a document.
    fun count(env: ScoringEnv): Int
    // Return an boolean for a document. (must be true if you want this to be ranked).
    fun matches(env: ScoringEnv): Boolean
    // Return an explanation for a document.
    fun explain(env: ScoringEnv): Explanation

    // Used to accelerate AND and OR matching if accurate.
    fun estimateDF(): Long
    fun setHeapMinimum(target: Double) {}

    fun visit(fn: (QueryEvalNode)->Unit) {
        fn(this)
        for (c in children) {
            c.visit(fn)
        }
    }
}

abstract class LeafEvalNode : QueryEvalNode {
    lateinit var env: ScoringEnv
    override val children: List<QueryEvalNode> = emptyList()
}

internal class FixedMatchEvalNode(val matchAnswer: Boolean, val df: Long = 0L): LeafEvalNode(), BooleanNode {
    override fun estimateDF(): Long = df
    override fun matches(env: ScoringEnv): Boolean = matchAnswer
    override fun explain(env: ScoringEnv): Explanation = if (matchAnswer) {
        Explanation.match(score(env).toFloat(), "AlwaysMatchLeaf")
    } else {
        Explanation.noMatch("NeverMatchLeaf")
    }
}

internal class WhitelistMatchEvalNode(val allowed: TIntHashSet): LeafEvalNode() {
    val N = allowed.size().toLong()
    override fun estimateDF(): Long = N
    override fun matches(env: ScoringEnv): Boolean = allowed.contains(env.doc)
    override fun score(env: ScoringEnv): Double = if (matches(env)) { 1.0 } else { 0.0 }
    override fun count(env: ScoringEnv): Int = if (matches(env)) { 1 } else { 0 }
    override fun explain(env: ScoringEnv): Explanation = if (matches(env)) {
        Explanation.match(score(env).toFloat(), "WhitelistMatchEvalNode N=$N")
    } else {
        Explanation.noMatch("WhitelistMatchEvalNode N=$N")
    }
}

// Going to execute this many times per document? Takes a while? Optimize that.
private class CachedQueryEvalNode(override val child: QueryEvalNode) : SingleChildEval<QueryEvalNode>() {
    var cachedScore = -Double.MAX_VALUE
    var cachedScoreDoc = -1
    var cachedCount = 0
    var cachedCountDoc = -1

    override fun score(env: ScoringEnv): Double {
        if (env.doc == cachedScoreDoc) {
            cachedScoreDoc = env.doc
            cachedScore = child.score(env)
        }
        return cachedScore
    }

    override fun count(env: ScoringEnv): Int {
        if (env.doc == cachedCountDoc) {
            cachedCountDoc = env.doc
            cachedCount = child.count(env)
        }
        return cachedCount
    }

    override fun explain(env: ScoringEnv): Explanation {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

interface CountEvalNode : QueryEvalNode {
    override fun score(env: ScoringEnv) = count(env).toDouble()
}
interface PositionsEvalNode : CountEvalNode {
    fun positions(env: ScoringEnv): PositionsIter
}

class ConstCountEvalNode(val count: Int, val lengths: QueryEvalNode) : LeafEvalNode(), CountEvalNode {
    override fun count(env: ScoringEnv): Int = count
    override fun matches(env: ScoringEnv): Boolean = lengths.matches(env)
    override fun explain(env: ScoringEnv): Explanation = Explanation.match(count.toFloat(), "ConstCountEvalNode", listOf(lengths.explain(env)))
    override fun estimateDF(): Long = lengths.estimateDF()
}

class ConstTrueNode(val numDocs: Int) : LeafEvalNode() {
    override fun setHeapMinimum(target: Double) { }
    override fun score(env: ScoringEnv): Double = 1.0
    override fun count(env: ScoringEnv): Int = 1
    override fun matches(env: ScoringEnv): Boolean = true
    override fun explain(env: ScoringEnv): Explanation = Explanation.match(1f, "ConstTrueNode")
    override fun estimateDF(): Long = numDocs.toLong()
}

interface BooleanNode : QueryEvalNode {
    override fun score(env: ScoringEnv): Double = if (matches(env)) 1.0 else 0.0
    override fun count(env: ScoringEnv): Int = if (matches(env)) 1 else 0
}

/**
 * Created from [ConstScoreExpr] via [exprToEval]
 */
class ConstEvalNode(val count: Int, val score: Double) : LeafEvalNode() {
    constructor(count: Int) : this(count, count.toDouble())
    constructor(score: Double) : this(1, score)

    override fun setHeapMinimum(target: Double) { }
    override fun score(env: ScoringEnv): Double = score
    override fun count(env: ScoringEnv): Int = count
    override fun matches(env: ScoringEnv): Boolean = false

    override fun explain(env: ScoringEnv): Explanation = Explanation.noMatch("ConstEvalNode(count=$count, score=$score)")
    override fun estimateDF(): Long = 0L
}

internal class CountEqualsNode(val count: Int, override val child: QueryEvalNode) : SingleChildEval<QueryEvalNode>(), BooleanNode {
    override fun matches(env: ScoringEnv): Boolean = child.matches(env) && (child.count(env) == count)
    override fun explain(env: ScoringEnv): Explanation = if(matches(env)) {
        Explanation.match(1.0f, "count=$count? YES: ${child.count(env)}", child.explain(env))
    } else {
        Explanation.match(1.0f, "count=$count? NO: ${child.count(env)}", child.explain(env))
    }
}

/**
 * Created from [RequireExpr] via [exprToEval]
 */
internal class RequireEval(val cond: QueryEvalNode, val score: QueryEvalNode): QueryEvalNode {
    override val children: List<QueryEvalNode> = listOf(cond, score)
    override fun score(env: ScoringEnv): Double = score.score(env)
    override fun count(env: ScoringEnv): Int = score.count(env)
    /**
     * Note: Galago semantics, don't look at whether score matches.
     * @see createOptimizedMovementExpr
     */
    override fun matches(env: ScoringEnv): Boolean = cond.matches(env)
    override fun explain(env: ScoringEnv): Explanation {
        val expls = listOf(cond, score).map { it.explain(env) }
        return if (cond.matches(env)) {
            Explanation.match(score.score(env).toFloat(), "require-match", expls)
        } else {
            Explanation.noMatch("${score.score(env)} for require-miss", expls)
        }
    }
    override fun setHeapMinimum(target: Double) { score.setHeapMinimum(target) }
    override fun estimateDF(): Long = minOf(score.estimateDF(), cond.estimateDF())
}

/**
 * Helper class to generate Lucene's [Explanation] for subclasses of [AndEval] and  [OrEval] like [WeightedSumEval] or even [OrderedWindow].
 */
abstract class RecursiveEval<out T : QueryEvalNode>(override val children: List<T>) : QueryEvalNode {
    val className = this.javaClass.simpleName
    val N = children.size
    lateinit var env: ScoringEnv
    override fun explain(env: ScoringEnv): Explanation {
        val expls = children.map { it.explain(env) }
        if (matches(env)) {
            return Explanation.match(score(env).toFloat(), "$className.Match", expls)
        }
        return Explanation.noMatch("$className.Miss", expls)
    }
}

/**
 * Created from [MultiExpr] via [exprToEval].
 */
class MultiEvalNode(children: List<QueryEvalNode>, val names: List<String>) : OrEval<QueryEvalNode>(children) {
    val primary: Int = Math.max(0, names.indexOf("primary"))
    override fun count(env: ScoringEnv): Int = children[primary].count(env)
    override fun score(env: ScoringEnv): Double = children[primary].score(env)

    override fun explain(env: ScoringEnv): Explanation {
        val expls = children.map { it.explain(env) }

        val namedChildExpls = names.zip(expls).map { (name, childExpl) ->
            if (childExpl.isMatch) {
                Explanation.match(childExpl.value, name, childExpl)
            } else {
                Explanation.noMatch("${childExpl.value} for name", childExpl)
            }
        }

        return Explanation.noMatch("MultiEvalNode ${names}", namedChildExpls)
    }
}

/**
 * Abstract class that knows how to match a set of children, optimized on their expected DF. Most useful query models are subclasses, e.g. [WeightedSumEval].
 */
abstract class OrEval<out T : QueryEvalNode>(children: List<T>) : RecursiveEval<T>(children) {
    val cost = children.map { it.estimateDF() }.max() ?: 0L
    val moveChildren = children.sortedByDescending { it.estimateDF() }
    override fun estimateDF(): Long = cost
    override fun matches(env: ScoringEnv): Boolean {
        return moveChildren.any { it.matches(env) }
    }
}

/** Note that unlike in Galago, [AndEval] nodes do not perform movement. They briefly optimize to answer matches(doc) faster on average, but movement comes from a different query-program, e.g., [AndMover] where all leaf iterators only have doc information and are cheap copies as a result. */
abstract class AndEval<out T : QueryEvalNode>(children: List<T>) : RecursiveEval<T>(children) {
    val cost = children.map { it.estimateDF() }.min() ?: 0L
    val moveChildren = children.sortedBy { it.estimateDF() }
    override fun estimateDF(): Long = cost
    override fun matches(env: ScoringEnv): Boolean {
        return moveChildren.all { it.matches(env) }
    }
}

/**
 * Created from [OrExpr] using [exprToEval]
 */
internal class BooleanOrEval(children: List<QueryEvalNode>): OrEval<QueryEvalNode>(children), BooleanNode { }
/**
 * Created from [AndExpr] using [exprToEval]
 */
internal class BooleanAndEval(children: List<QueryEvalNode>): AndEval<QueryEvalNode>(children), BooleanNode { }

/**
 * Created from [MaxExpr] using [exprToEval]
 */
internal class MaxEval(children: List<QueryEvalNode>) : OrEval<QueryEvalNode>(children) {
    override fun score(env: ScoringEnv): Double {
        var sum = 0.0
        children.forEach {
            sum = maxOf(sum, it.score(env))
        }
        return sum
    }
    override fun count(env: ScoringEnv): Int {
        var sum = 0
        children.forEach {
            sum = maxOf(sum, it.count(env))
        }
        return sum
    }
    // Getting over the "min" is the same for any child of a max node.
    override fun setHeapMinimum(target: Double) {
        children.forEach { it.setHeapMinimum(target) }
    }
}

/**
 * Created from [CombineExpr] using [exprToEval]
 * Also known as #combine for you galago/indri folks.
 */
internal class WeightedSumEval(children: List<QueryEvalNode>, val weights: DoubleArray) : OrEval<QueryEvalNode>(children) {
    override fun score(env: ScoringEnv): Double {
        return (0 until children.size).sumByDouble {
            weights[it] * children[it].score(env)
        }
    }

    override fun explain(env: ScoringEnv): Explanation {
        val expls = children.map { it.explain(env) }
        if (matches(env)) {
            return Explanation.match(score(env).toFloat(), "$className.Match ${weights.toList()}", expls)
        }
        return Explanation.noMatch("$className.Miss ${weights.toList()}", expls)
    }

    override fun count(env: ScoringEnv): Int = error("Calling counts on WeightedSumEval is nonsense.")
    init { assert(weights.size == children.size, {"Weights provided to WeightedSumEval must exist for all children."}) }
}

/**
 * Created from [CombineExpr] using [exprToEval] iff all children are DirQLExpr.
 * The JIT is much more likely to vectorize log expressions in a loop than past a virtual call.
 * Also known as #combine for you galago/indri folks.
 */
internal class WeightedLogSumEval(children: List<QueryEvalNode>, val weights: DoubleArray) : OrEval<QueryEvalNode>(children) {
    override fun score(env: ScoringEnv): Double {
        return (0 until children.size).sumByDouble {
            //weights[it] * ApproxLog.faster_log(children[it].score())
            weights[it] * Math.log(children[it].score(env))
        }
    }

    override fun explain(env: ScoringEnv): Explanation {
        val expls = children.map { it.explain(env) }
        if (matches(env)) {
            return Explanation.match(score(env).toFloat(), "$className.Match ${weights.toList()}", expls)
        }
        return Explanation.noMatch("$className.Miss ${weights.toList()}", expls)
    }

    override fun toString(): String {
        return children.zip(weights.toList()).joinToString(prefix="(", separator=" + ", postfix=")") { (c,w) -> "log($w * $c)" }
    }

    override fun count(env: ScoringEnv): Int = error("Calling counts on WeightedLogSumEval is nonsense.")
    init { assert(weights.size == children.size, {"Weights provided to WeightedLogSumEval must exist for all children."}) }
}

/**
 * Helper class to make scorers that will have one count [child] and a [lengths] child (like [DirichletSmoothingEval] and [BM25ScoringEval]) easier to implement.
 */
internal abstract class ScorerEval : QueryEvalNode {
    abstract val child: QueryEvalNode
    abstract val lengths: QueryEvalNode

    override val children: List<QueryEvalNode> get() = listOf(child, lengths)
    override fun estimateDF(): Long = child.estimateDF()
    override fun matches(env: ScoringEnv): Boolean = child.matches(env)
}

internal abstract class SingleChildEval<out T : QueryEvalNode> : QueryEvalNode {
    abstract val child: T
    override val children: List<QueryEvalNode> get() = listOf(child)
    override fun estimateDF(): Long = child.estimateDF()
    override fun matches(env: ScoringEnv): Boolean = child.matches(env)
}

internal class WeightedEval(override val child: QueryEvalNode, val weight: Double): SingleChildEval<QueryEvalNode>() {
    override fun setHeapMinimum(target: Double) {
        // e.g., if this is 2*child, and target is 5
        // child target is 5 / 2
        child.setHeapMinimum(target / weight)
    }

    override fun score(env: ScoringEnv): Double = weight * child.score(env)
    override fun count(env: ScoringEnv): Int = error("Weighted($weight).count()")
    override fun explain(env: ScoringEnv): Explanation {
        val orig = child.score(env)
        return if (child.matches(env)) {
            Explanation.match(score(env).toFloat(), "Weighted@${env.doc} = $weight * $orig", child.explain(env))
        } else {
            Explanation.noMatch("Weighted.Miss@${env.doc} (${weight*orig} = $weight * $orig)", child.explain(env))
        }
    }
}

/**
 * Created from [BM25Expr] via [exprToEval]
 */
internal class BM25ScoringEval(override val child: QueryEvalNode, override val lengths: QueryEvalNode, val b: Double, val k: Double, val stats: CountStats): ScorerEval() {
    private val avgDL = stats.avgDL()
    private val idf = Math.log(stats.dc / (stats.df + 0.5))

    override fun score(env: ScoringEnv): Double {
        val count = child.count(env).toDouble()
        val length = lengths.count(env).toDouble()
        val num = count * (k+1.0)
        val denom = count + (k * (1.0 - b + (b * length / avgDL)))
        return idf * (num / denom)
    }

    override fun count(env: ScoringEnv): Int = error("count() not implemented for ScoreNode")
    override fun explain(env: ScoringEnv): Explanation {
        val c = child.count(env)
        val length = lengths.count(env)
        if (c > 0) {
            return Explanation.match(score(env).toFloat(), "$c/$length with b=$b, k=$k with BM25. ${stats}", listOf(child.explain(env)))
        } else {
            return Explanation.noMatch("score=${score(env)} or $c/$length with b=$b, k=$k with BM25. ${stats}", listOf(child.explain(env)))
        }
    }
}

/**
 * BM25 does have an advantage over QL: you can optimize the snot out of it.
 * The naive equation has many elements that can be precomputed.
 *
 * val num = count * (k+1.0)
 * val denom = count + (k * (1.0 - b + (b * length / avgDL)))
 * Naive: 3 multiplies, 1 division, 3 additions, 1 subtraction
 *
 * val num = count * KPlusOne
 * val denom = count + KTimesOneMinusB + KTimesBOverAvgDL * length
 * Optimized: 2 multiplies, 2 additions
 *
 * QL, on the other hand:
 * return Math.log((c + background) / (length + mu))
 * 2 additions, 1 division, and a LOG.
 *
 */
internal class BM25InnerScoringEval(override val child: QueryEvalNode, override val lengths: QueryEvalNode, val b: Double, val k: Double, val stats: CountStats): ScorerEval() {
    private val avgDL = stats.avgDL()

    //val num = count * (k+1.0)
    //val denom = count + (k * (1.0 - b + (b * length / avgDL)))
    val OneMinusB = 1.0 - b
    val KPlusOne = k+1.0
    val BOverAvgDL = b / avgDL

    // val denom = count + (k * (OneMinusB + (BOverAvgDL * length)))
    // val denom = count + (k*OneMinuB + k*BOverAvgDL*length)
    val KTimesOneMinusB = k * OneMinusB
    val KTimesBOverAvgDL = k * BOverAvgDL

    override fun score(env: ScoringEnv): Double {
        val count = child.count(env).toDouble()
        val length = lengths.count(env).toDouble()
        val num = count * KPlusOne
        val denom = count + KTimesOneMinusB + KTimesBOverAvgDL * length
        return num / denom
    }

    override fun count(env: ScoringEnv): Int = error("count() not implemented for ScoreNode")
    override fun explain(env: ScoringEnv): Explanation {
        val c = child.count(env)
        val length = lengths.count(env)
        if (c > 0) {
            return Explanation.match(score(env).toFloat(), "$c/$length with b=$b, k=$k with BM25Inner. ${stats}", listOf(child.explain(env)))
        } else {
            return Explanation.noMatch("score=${score(env)} or $c/$length with b=$b, k=$k with BM25Inner. ${stats}", listOf(child.explain(env)))
        }
    }
}

/**
 * Created from [DirQLExpr] via [exprToEval]
 */
internal class DirichletSmoothingEval(override val child: QueryEvalNode, override val lengths: QueryEvalNode, val mu: Double, val stats: CountStats) : ScorerEval() {
    val background = mu * stats.nonzeroCountProbability()
    init {
        assert(java.lang.Double.isFinite(background)) { "stats=$stats" }
    }
    override fun score(env: ScoringEnv): Double {
        val c = child.count(env).toDouble()
        val length = lengths.count(env).toDouble()
        return Math.log((c + background) / (length + mu))
    }
    override fun count(env: ScoringEnv): Int = TODO("not yet")
    override fun explain(env: ScoringEnv): Explanation {
        val c = child.count(env)
        val length = lengths.count(env)
        if (c > 0) {
            return Explanation.match(score(env).toFloat(), "$c/$length with mu=$mu, bg=$background dirichlet smoothing. $stats", listOf(child.explain(env)))
        } else {
            return Explanation.noMatch("score=${score(env)} or $c/$length with mu=$mu, bg=$background dirichlet smoothing $stats ${stats.nonzeroCountProbability()}.", listOf(child.explain(env)))
        }
    }
}

/**
 * Created from [DirQLExpr] via [exprToEval] sometimes, inside of [WeightedLogSumEval]
 */
internal class NoLogDirichletSmoothingEval(override val child: QueryEvalNode, override val lengths: QueryEvalNode, val mu: Double, val stats: CountStats) : ScorerEval() {
    val background = mu * stats.nonzeroCountProbability()
    init {
        assert(java.lang.Double.isFinite(background)) { "stats=$stats" }
    }
    override fun score(env: ScoringEnv): Double {
        val c = child.count(env).toDouble()
        val length = lengths.count(env).toDouble()
        return (c + background) / (length + mu)
    }
    override fun count(env: ScoringEnv): Int = TODO("not yet")
    override fun explain(env: ScoringEnv): Explanation {
        val c = child.count(env)
        val length = lengths.count(env)
        if (c > 0) {
            return Explanation.match(score(env).toFloat(), "$c/$length with mu=$mu, bg=$background dirichlet smoothing. ${stats}", listOf(child.explain(env)))
        } else {
            return Explanation.noMatch("score=${score(env)} or $c/$length with mu=$mu, bg=$background dirichlet smoothing $stats ${stats.nonzeroCountProbability()}.", listOf(child.explain(env)))
        }
    }

    override fun toString(): String {
        return "dir($child)"
    }
}

object DirichletSmoothingExploration {
    @JvmStatic fun main(args: Array<String>) {
        // As length increases, so does the Dirichlet Probability.
        // As frequency increases, so does the Dirichlet Probability.
        // Need max length.
        // Estimate max freq as a fraction of that max length?
        val bg = 0.05
        val mu = 1500.0
        (0 .. 100).forEach { i ->
            val f = Fraction(i, 100)
            val values = (0 .. 10).map { s ->
                val count = (f.numerator * s).toDouble()
                val length = (f.denominator * s).toDouble()

                Math.log((count + bg) / (length + mu))
            }

            println("${f.numerator}/${f.denominator} ${values.joinToString { "%1.3f".format(it) }}")
        }
    }
}

