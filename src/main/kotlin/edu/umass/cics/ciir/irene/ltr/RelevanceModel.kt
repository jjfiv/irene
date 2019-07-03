package edu.umass.cics.ciir.irene.ltr

import edu.umass.cics.ciir.irene.lang.DirQLExpr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.SumExpr
import edu.umass.cics.ciir.irene.lang.TextExpr
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap


interface LanguageModel<T> {
    val length: Double
    fun prob(term: T): Double
    fun count(term: T): Int
    fun toTerms(): List<WeightedTerm>
    fun termSet(): Set<T>

    fun toTerms(k: Int): List<WeightedTerm> = toTerms().sorted().take(k).normalized()
    fun toQExpr(k: Int, scorer: (TextExpr)-> QExpr = { DirQLExpr(it) }, targetField: String? = null, statsField: String? = null) = SumExpr(toTerms(k).map {
        scorer(TextExpr(it.term, field = targetField, statsField = statsField)).weighted(it.score)
    })

}
interface LanguageModelBuilder<T> : LanguageModel<T> {
    fun increment(term: T, amount: Double=1.0)
    fun increment(other: LanguageModel<T>, stopwords: Set<T> = emptySet())
}

class MutableBagOfWords : LanguageModelBuilder<String> {
    private var totalWeight: Double = 0.0
    private var hashMap = TObjectDoubleHashMap<String>()
    override val length: Double get() = totalWeight

    override fun prob(term: String): Double = hashMap[term] / totalWeight
    override fun count(term: String): Int = hashMap[term].toInt()
    override fun toTerms(): List<WeightedTerm> {
        val output = ArrayList<WeightedTerm>(hashMap.size())
        hashMap.forEachEntry {term, weight ->
            output.add(WeightedTerm(weight / length, term))
        }
        return output
    }
    override fun termSet(): Set<String> = hashMap.keySet()
    override fun increment(term: String, amount: Double) {
        hashMap.adjustOrPutValue(term, amount, amount)
        totalWeight += amount
    }
    override fun increment(other: LanguageModel<String>, stopwords: Set<String>) {
        if (other is MutableBagOfWords) {
            other.hashMap.forEachEntry { term, weight ->
                if (!stopwords.contains(term)) {
                    hashMap.adjustOrPutValue(term, weight, weight)
                    totalWeight += weight
                }
                true
            }
        } else if (other is BagOfWords) {
            other.counts.forEachEntry { term, count ->
                if (!stopwords.contains(term)) {
                    val weight = count.toDouble()
                    hashMap.adjustOrPutValue(term, weight, weight)
                    totalWeight += weight
                }
                true
            }
        } else {
            for (key in other.termSet()) {
                if (stopwords.contains(key)) {
                    continue
                }
                increment(key, other.prob(key))
            }
        }
    }

}

class BagOfWords(val counts: TObjectIntHashMap<String>) : LanguageModel<String> {
    override fun termSet(): Set<String> = counts.keySet()
    constructor(terms: List<String>) : this(TObjectIntHashMap<String>().apply {
        terms.forEach { adjustOrPutValue(it, 1, 1) }
    })
    override val length: Double by lazy { counts.values().sum().toDouble() }
    val l2norm: Double by lazy {
        var sumSq = 0.0
        counts.forEachValue { c ->
            sumSq += c*c
            true
        }
        Math.sqrt(sumSq)
    }
    override fun prob(term: String): Double = counts.get(term) / length
    override fun count(term: String): Int {
        if (!counts.containsKey(term)) return 0
        return counts[term]
    }
    override fun toTerms(): List<WeightedTerm> {
        val output = ArrayList<WeightedTerm>(counts.size())
        counts.forEachEntry {term, weight ->
            output.add(WeightedTerm(weight.toDouble() / length, term))
        }
        return output
    }

}

data class WeightedItem<T>(val score: Double, val item: T) : Comparable<WeightedItem<T>> {
    // Natural order: biggest first.
    override fun compareTo(other: WeightedItem<T>): Int {
        return -java.lang.Double.compare(score, other.score)
    }
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

fun <T> List<WeightedItem<T>>.normalizedItems(): List<WeightedItem<T>> {
    val total = this.sumByDouble { it.score }
    return this.map { WeightedItem(it.score / total, it.item) }
}

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

