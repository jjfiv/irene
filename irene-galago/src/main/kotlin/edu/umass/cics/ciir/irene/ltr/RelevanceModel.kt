package edu.umass.cics.ciir.irene.ltr

import edu.umass.cics.ciir.irene.lang.*
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap




data class WeightedItem<T>(val score: Double, val item: T) : Comparable<WeightedItem<T>> {
    // Natural order: biggest first.
    override fun compareTo(other: WeightedItem<T>): Int {
        return -java.lang.Double.compare(score, other.score)
    }
}



fun <T> List<WeightedItem<T>>.normalizedItems(): List<WeightedItem<T>> {
    val total = this.sumByDouble { it.score }
    return this.map { WeightedItem(it.score / total, it.item) }
}

