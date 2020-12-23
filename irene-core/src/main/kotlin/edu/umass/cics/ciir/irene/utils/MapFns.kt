package edu.umass.cics.ciir.irene.utils

/**
 *
 * @author jfoley.
 */
fun<K> MutableMap<K, Double>.incr(key: K, amount: Double) {
    this.compute(key) { _,prev -> (prev ?: 0.0) + amount }
}
fun<K> MutableMap<K, Int>.incr(key: K, amount: Int): Int {
    return this.compute(key) { _,prev -> (prev ?: 0) + amount }!!
}
