package edu.umass.cics.ciir.irene.utils

import kotlin.math.*

/**
 * Ideas taken from:
 * http://www.johndcook.com/blog/standard_deviation/
 * @author jfoley
 */
class StreamingStats {
    var n: Long = 0
        private set
    var mean: Double = 0.toDouble()
        private set
    private var sValue: Double = 0.toDouble()
    var max: Double = 0.toDouble()
        private set
    var min: Double = 0.toDouble()
        private set
    var total: Double = 0.toDouble()
        private set
    val variance: Double
        get() = if (n <= 1) 0.0 else sValue / (n - 1).toDouble()
    val standardDeviation: Double
        get() = sqrt(variance)
    val count: Double
        get() = n.toDouble()

    val isEmpty: Boolean
        get() = n == 0L

    init {
        clear()
    }

    fun push(i: Int) = push(i.toDouble())
    fun push(f: Float) = push(f.toDouble())
    fun push(l: Long) = push(l.toDouble())
    fun push(d: Double) {
        n++

        // set up for next iteration
        val oldMean = mean
        val oldS = sValue

        max = max(max, d)
        min = min(min, d)
        total += d

        // See Knuth TAOCP vol 2, 3rd edition, page 232
        if (n == 1L) {
            mean = d
            return
        }

        mean = oldMean + (d - oldMean) / n.toDouble()
        sValue = oldS + (d - oldMean) * (d - mean)
    }

    /**
     * Not lossless. The streaming method does better.
     * @param other built up statistics.
     */
    fun add(other: StreamingStats) {
        val total = this.n + other.n
        val lhsFrac = this.n / total.toDouble()
        val rhsFrac = other.n / total.toDouble()

        val newMean = lhsFrac * this.mean + rhsFrac * other.mean

        val delta = other.mean - this.mean
        val delta2 = delta * delta

        val newS = this.sValue + other.sValue + delta2 * lhsFrac * rhsFrac

        this.n = total
        this.mean = newMean
        this.sValue = newS
        this.max = max(this.max, other.max)
        this.min = min(this.min, other.min)
        this.total += other.total
    }

    fun clear() {
        total = 0.0
        n = 0
        sValue = 0.0
        mean = sValue
        max = -java.lang.Double.MAX_VALUE
        min = java.lang.Double.MAX_VALUE
    }

    fun count(): Long {
        return n
    }

    val features: Map<String, Double>
        get() = mapOf(Pair("mean", mean),
                Pair("variance", variance),
                Pair("stddev", standardDeviation),
                Pair("max", max),
                Pair("min", min),
                Pair("total", total),
                Pair("count", count))

    override fun toString(): String {
        return if (count() > 0) { features.toString() } else "EMPTY"
    }

    fun pushAll(data: FloatArray) {
        for (v in data) {
            push(v.toDouble())
        }
    }

    fun pushAll(select: List<Double>): StreamingStats {
        for (x in select) {
            push(x)
        }
        return this
    }
    fun toComputedStats(): ComputedStats = ComputedStats(this)
    fun maxMinNormalize(rawVal: Double): Double {
        return (rawVal - min) / (max - min)
    }
}

data class ComputedStats(val mean: Double, val min: Double, val max: Double, val variance: Double, val stddev: Double, val total: Double, val count: Double) {
    constructor(map: Map<String, Double>) : this(
            map["mean"]!!,
            map["min"]!!,
            map["max"]!!,
            map["variance"]!!,
            map["stddev"]!!,
            map["total"]!!,
            map["count"]!!)
    constructor(rhs: StreamingStats): this (rhs.mean, rhs.min, rhs.max, rhs.variance, rhs.standardDeviation, rhs.total, rhs.count)

    val features: Map<String, Double>
        get() = mapOf(Pair("mean", mean),
                Pair("variance", variance),
                Pair("stddev", stddev),
                Pair("max", max),
                Pair("min", min),
                Pair("total", total),
                Pair("count", count))
}

fun safeDiv(x: Int, y: Int): Double = if (x == 0 || y == 0) 0.0 else x.toDouble() / y.toDouble()

/**
 * Computes the weighted average of scores: -> log( w0 * exp(score[0]) + w1 *
 * exp(score[1]) + w1 * exp(score[2]) + .. )
 *
 * to avoid rounding errors, we compute the equivalent expression:
 *
 * returns: maxScore + log( w0 * exp(score[0] - max) + w1 * exp(score[1] -
 * max) + w2 * exp(score[2] - max) + .. )
 */
fun weightedLogSumExp(weights: DoubleArray, scores: DoubleArray): Double {
    if (scores.isEmpty()) {
        throw RuntimeException("weightedLogSumExp was called with a zero length array of scores.")
    }

    // find max value - this score will dominate the final score
    var max = Double.NEGATIVE_INFINITY
    for (score in scores) {
        max = max(score, max)
    }
    var sum = 0.0
    for (i in scores.indices) {
        sum += weights[i] * exp(scores[i] - max)
    }
    sum = max + ln(sum)
    return sum
}

/**
 * Computes the sum of scores: -> log( exp(score[0]) + exp(score[1]) + exp(score[2]) + .. )
 *
 * to avoid rounding errors, we compute the equivalent expression:
 *
 * returns: maxScore + log( exp(score[0] - max) + exp(score[1] -
 * max) + exp(score[2] - max) + .. )
 */
fun logSumExp(scores: DoubleArray): Double {
    if (scores.isEmpty()) {
        throw RuntimeException("logSumExp was called with a zero length array of scores.")
    }

    // find max value - this score will dominate the final score
    val max = scores.max() ?: Double.NEGATIVE_INFINITY
    var sum = 0.0
    for (i in scores.indices) {
        sum += exp(scores[i] - max)
    }
    sum = max + ln(sum)
    return sum
}
