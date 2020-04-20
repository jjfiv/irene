package edu.umass.cics.ciir.irene.lucene

import edu.umass.cics.ciir.irene.lang.CountStats
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.RREnv

/**
 *
 * @author jfoley.
 */
sealed class CountStatsStrategy {
    abstract fun get(): CountStats
}
class LazyCountStats(val expr: QExpr, val env: RREnv) : CountStatsStrategy() {
    private val stats: CountStats by lazy { env.computeStats(expr) }
    override fun get(): CountStats = stats
}
class ExactEnvStats(env: RREnv, val term: String, val field: String) : CountStatsStrategy() {
    val stats = env.getStats(term,  field)
    override fun get(): CountStats = stats
}
inline fun <T> List<T>.lazyIntMin(func: (T)->Int): Int? {
    var curMin: Int? = null
    for (x in this) {
        val cur = func(x)
        if (curMin == null || (curMin > cur)) {
            curMin = cur
        }
    }
    return curMin
}
inline fun <T> List<T>.lazyMinAs(func: (T)->Long): Long? {
    var curMin: Long? = null
    for (x in this) {
        val cur = func(x)
        if (curMin == null || (curMin > cur)) {
            curMin = cur
        }
    }
    return curMin
}
class MinEstimatedCountStats(expr: QExpr, cstats: List<CountStats>): CountStatsStrategy() {
    override fun get(): CountStats = estimatedStats
    private val estimatedStats = CountStats("Min($expr)",
            cstats.lazyMinAs { it.cf }?:0,
            cstats.lazyMinAs { it.df }?:0, cstats[0].cl, cstats[0].dc)
}
class ProbEstimatedCountStats(expr: QExpr, cstats: List<CountStats>): CountStatsStrategy() {
    override fun get(): CountStats = estimatedStats
    private val estimatedStats = CountStats("Prob($expr)",
            0,0,cstats[0].cl, cstats[0].dc).apply {
        if (cstats.lazyMinAs { it.cf } ?: 0L == 0L) {
            // one term does not exist, all things are zero.
        } else {
            cf = Math.exp(
                    // size of collection
                    Math.log(cl.toDouble()) +
                            // multiply probabilities together in logspace / term-independence model
                            cstats.map { Math.log(it.countProbability()) }.sum()).toLong()
            df = Math.exp(
                    // size of collection (# docs)
                    Math.log(df.toDouble()) +
                            // multiply probabilities together in logspace / term-independence model
                            cstats.map { Math.log(it.binaryProbability()) }.sum()).toLong()
        }
    }
}

