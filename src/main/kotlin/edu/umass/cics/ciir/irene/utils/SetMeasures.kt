package edu.umass.cics.ciir.irene.utils

/**
 * @author jfoley
 */

data class Fraction(val numerator: Int, val denominator: Int) {
    val value: Double = safeDiv(numerator, denominator)
}

fun <T> Recall(truth: Set<T>, pred: Set<T>) = Fraction(truth.count { pred.contains(it) }, truth.size)
fun <T> Precision(truth: Set<T>, pred: Set<T>) = Fraction(truth.count { pred.contains(it) }, pred.size)
fun <T> F1(truth: Set<T>, pred: Set<T>): Double {
    val correct = truth.count { pred.contains(it) }
    if (correct == 0) return 0.0
    val p = safeDiv(correct, pred.size)
    val r = safeDiv(correct, truth.size)
    return 2.0 * (p * r) / (p + r)
}
