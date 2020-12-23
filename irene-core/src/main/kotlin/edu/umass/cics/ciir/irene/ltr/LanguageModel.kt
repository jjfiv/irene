package edu.umass.cics.ciir.irene.ltr

import edu.umass.cics.ciir.irene.lang.*

/**
 *
 * @author jfoley.
 */
interface LanguageModel<T> {
    val length: Double
    fun prob(term: T): Double
    fun count(term: T): Int
    fun toTerms(): List<WeightedTerm>
    fun termSet(): Set<T>

    fun toTerms(k: Int): List<WeightedTerm> = toTerms().sorted().take(k).normalized()
    fun toQExpr(k: Int, scorer: (TextExpr)-> QExpr = { DirQLExpr(it) }, targetField: String? = null, statsField: String? = null, discountStatsEnv: RREnv? = null) = SumExpr(toTerms(k).map {
        val node = scorer(TextExpr(it.term, field = targetField, statsField = statsField))
        if (discountStatsEnv == null) {
            node.weighted(it.score)
        } else {
            node.weighted(it.score / discountStatsEnv.getStats(it.term, statsField).nonzeroCountProbability())
        }
    })
}

