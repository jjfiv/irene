package edu.umass.cics.ciir.irene.ltr

import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.DirQLExpr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.SumExpr
import edu.umass.cics.ciir.irene.lang.TextExpr
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import gnu.trove.map.hash.TObjectDoubleHashMap
import org.lemurproject.galago.utility.MathUtils


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

data class RelevanceModel(val weights: TObjectDoubleHashMap<String>, val sourceField: String?) {
    private fun toTerms(): List<WeightedTerm> {
        val output = ArrayList<WeightedTerm>(weights.size())
        weights.forEachEntry {term, weight ->
            output.add(WeightedTerm(weight, term, sourceField ?: ""))
        }
        return output
    }
    fun toTerms(k: Int): List<WeightedTerm> = toTerms().sorted().take(k).normalized()
    fun toQExpr(k: Int, scorer: (TextExpr)-> QExpr = { DirQLExpr(it) }, targetField: String? = null, statsField: String? = null) = SumExpr(toTerms(k).map { scorer(TextExpr(it.term, targetField ?: sourceField, statsField = statsField)).weighted(it.score) })
}

