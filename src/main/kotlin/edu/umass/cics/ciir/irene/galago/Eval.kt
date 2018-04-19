package edu.umass.cics.ciir.irene.galago

import edu.umass.cics.ciir.irene.utils.StreamingStats
import gnu.trove.list.array.TDoubleArrayList
import org.lemurproject.galago.core.eval.metric.QueryEvaluator
import org.lemurproject.galago.core.eval.metric.QueryEvaluatorFactory
import org.lemurproject.galago.utility.Parameters
import java.util.*

/**
 * @author jfoley
 */

open class KeyedMeasure<K> {
    val measures = HashMap<K, StreamingStats>()
    fun push(what: K, x: Double) {
        this[what].push(x)
    }
    fun ppush(ctx: String, what: K, x: Double) {
        println("$ctx\t$what\t${"%1.3f".format(x)}")
        this[what].push(x)
    }
    operator fun get(index: K): StreamingStats = measures.computeIfAbsent(index, { StreamingStats() })
    open fun means(): Map<K, Double> = measures.mapValues { (_,arr) -> arr.mean }
}
class NamedMeasures : KeyedMeasure<String>() {
    override fun means(): TreeMap<String, Double> = measures.mapValuesTo(TreeMap()) { (_,arr) -> arr.mean }

    override fun toString(): String {
        return means().entries.joinToString(separator = "\t") { (k,v) -> "%s=%1.3f".format(k, v) }
    }
}

fun getEvaluators(vararg metricNames: String) = getEvaluators(metricNames.toList())
fun getEvaluators(metricNames: List<String>) = metricNames.associate { Pair(it, QueryEvaluatorFactory.create(it, Parameters.create())!!) }

fun getEvaluator(name: String): QueryEvaluator =
        QueryEvaluatorFactory.create(name, Parameters.create())!!

val LN2 = Math.log(2.0)
fun log2(x: Double): Double = Math.log(x) / LN2

fun TDoubleArrayList.mean(): Double {
    if (this.size() == 0) return 0.0;
    return this.sum() / this.size().toDouble()
}

fun TDoubleArrayList.logProb(orElse: Double): Double {
    if (this.size() == 0) return orElse;
    return Math.log(this.sum() / this.size().toDouble())
}

fun<K> MutableMap<K, Double>.incr(key: K, amount: Double) {
    this.compute(key) { _,prev -> (prev ?: 0.0) + amount }
}
fun<K> MutableMap<K, Int>.incr(key: K, amount: Int): Int {
    return this.compute(key) { _,prev -> (prev ?: 0) + amount }!!
}
