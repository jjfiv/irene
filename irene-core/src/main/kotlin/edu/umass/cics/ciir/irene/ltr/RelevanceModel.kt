package edu.umass.cics.ciir.irene.ltr

import gnu.trove.map.hash.TObjectDoubleHashMap

/**
 *
 * @author jfoley.
 */
data class RelevanceModel(val weights: TObjectDoubleHashMap<String>, val sourceField: String?) : LanguageModel<String> {
    override fun termSet(): Set<String> = weights.keySet()
    override val length: Double by lazy { weights.values().sum() }
    override fun prob(term: String): Double = weights.get(term)
    override fun count(term: String): Int = throw UnsupportedOperationException()
    override fun toTerms(): List<WeightedTerm> {
        val output = ArrayList<WeightedTerm>(weights.size())
        weights.forEachEntry {term, weight ->
            output.add(WeightedTerm(weight, term, sourceField ?: ""))
        }
        return output
    }
}

