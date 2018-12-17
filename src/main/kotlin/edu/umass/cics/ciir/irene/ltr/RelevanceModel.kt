package edu.umass.cics.ciir.irene.ltr

import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.DirQLExpr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.SumExpr
import edu.umass.cics.ciir.irene.lang.TextExpr
import gnu.trove.map.hash.TObjectDoubleHashMap
import org.lemurproject.galago.utility.MathUtils


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

