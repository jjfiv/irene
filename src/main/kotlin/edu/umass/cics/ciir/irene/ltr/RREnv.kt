package edu.umass.cics.ciir.irene.ltr

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.irene.scoring.LTREvalSetupContext
import edu.umass.cics.ciir.irene.scoring.exprToEval
import gnu.trove.map.hash.TObjectDoubleHashMap
import org.lemurproject.galago.utility.MathUtils

abstract class RREnv {
    open var defaultField = "document"
    open var defaultDirichletMu = 1500.0
    open var defaultBM25b = 0.75
    open var defaultBM25k = 1.2
    open var absoluteDiscountingDelta = 0.7
    open var estimateStats: String? = "min"
    open var optimizeMovement = true
    open var shareIterators: Boolean = true
    open var optimizeBM25 = false
    open var optimizeDirLog = false
    open var indexedBigrams = false

    val ltrContext = LTREvalSetupContext(this)
    fun makeLTRQuery(q: QExpr) = exprToEval(prepare(q), ltrContext)

    // nullable so it can be used to determine if this index has the given field.
    abstract fun fieldStats(field: String): CountStats
    abstract fun computeStats(q: QExpr): CountStats
    abstract fun getStats(term: String, field: String? =null): CountStats

    /** @return a list of internal identifiers */
    abstract fun lookupNames(docNames: Set<String>): List<Int>

    fun prepare(q: QExpr): QExpr {
        var pq =
                simplify(q)
                .map { reduceSingleChildren(it) }
        applyEnvironment(this, pq)
        try {
            analyzeDataNeededRecursive(pq)
        } catch (err: TypeCheckError) {
            System.err.println("$q")
            throw err
        }
        pq = insertStats(this, pq)

        // Optimize BM25:
        if (optimizeBM25) {
            // Only our system supports "lifting" the idf out of a BM25Expr.
            val bq = pq.map { c ->
                if (c is BM25Expr && !c.extractedIDF) {
                    val inner = c.child
                    val stats = c.stats
                    if (inner is TextExpr && stats != null) {
                        c.extractedIDF = true
                        val idf = Math.log(stats.dc / (stats.df + 0.5))
                        //println("IDF: ${idf} for term=${inner.text}")
                        c.weighted(idf)
                    } else {
                        c
                    }
                } else {
                    c
                }
            }
            return simplify(bq)
        } else {
            return simplify(pq)
        }
    }


    fun mean(exprs: List<RRExpr>) = RRMean(this, exprs)
    fun sum(exprs: List<RRExpr>) = RRSum(this, exprs)
    fun feature(name: String) = RRFeature(this, name)

    fun mult(vararg exprs: RRExpr) = RRMult(this, exprs.toList())
    fun const(x: Double) = RRConst(this, x)
}

fun computeRelevanceModel(docs: List<LTRDoc>, feature: String, fbDocs: Int, field: String, flat: Boolean = false, stopwords: Set<String> = inqueryStop, logSumExp:Boolean=false): RelevanceModel {
    val fbdocs = docs.sortedByDescending {
        it.features[feature] ?: error("Missing Feature! $feature in $it")
    }.take(fbDocs)

    val scores = fbdocs.map { it.features[feature]!! }
    val priors = if (!flat && logSumExp && scores.isNotEmpty()) {
        val norm = MathUtils.logSumExp(scores.toDoubleArray())
        scores.map { Math.exp(it-norm) }
    } else {
        scores
    }

    val rmModel = TObjectDoubleHashMap<String>()
    fbdocs.forEachIndexed { i, doc ->
        val local = doc.field(field).freqs.counts
        val length = doc.field(field).freqs.length

        val prior = if (flat) 1.0 else priors[i]
        local.forEachEntry {term, count ->
            if (stopwords.contains(term)) return@forEachEntry true
            val prob = prior * count.toDouble() / length
            rmModel.adjustOrPutValue(term, prob, prob)
            true
        }
    }

    return RelevanceModel(rmModel, field)
}

data class WeightedTerm(val score: Double, val term: String, val field: String = "") : Comparable<WeightedTerm> {
    // Natural order: biggest first.
    override fun compareTo(other: WeightedTerm): Int {
        var cmp = -java.lang.Double.compare(score, other.score)
        if (cmp != 0) return cmp
        cmp = field.compareTo(other.field)
        if (cmp != 0) return cmp
        return term.compareTo(other.term)
    }
}

fun List<WeightedTerm>.normalized(): List<WeightedTerm> {
    val total = this.sumByDouble { it.score }
    return this.map { WeightedTerm(it.score / total, it.term) }
}

data class RelevanceModel(val weights: TObjectDoubleHashMap<String>, val sourceField: String) {
    private fun toTerms(): List<WeightedTerm> {
        val output = ArrayList<WeightedTerm>(weights.size())
        weights.forEachEntry {term, weight ->
            output.add(WeightedTerm(weight, term, sourceField))
        }
        return output
    }
    fun toTerms(k: Int): List<WeightedTerm> = toTerms().sorted().take(k).normalized()
    fun toQExpr(k: Int, scorer: (TextExpr)-> QExpr = { DirQLExpr(it) }, targetField: String? = null, statsField: String? = null) = SumExpr(toTerms(k).map { scorer(TextExpr(it.term, targetField ?: sourceField, statsField = statsField)).weighted(it.score) })
}

