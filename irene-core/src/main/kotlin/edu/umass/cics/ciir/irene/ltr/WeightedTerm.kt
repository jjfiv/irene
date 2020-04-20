package edu.umass.cics.ciir.irene.ltr

/**
 *
 * @author jfoley.
 */

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
