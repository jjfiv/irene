package edu.umass.cics.ciir.irene.utils

import edu.umass.cics.ciir.irene.galago.incr
import java.util.*
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import kotlin.streams.toList

/**
 * @author jfoley
 */

fun <T>  Iterable<T>.shuffled(rand: Random = Random()): ArrayList<T> {
    val x = this.toCollection(ArrayList<T>())
    Collections.shuffle(x, rand)
    return x
}
fun <T, L: MutableList<T>> L.shuffle(rand: Random = Random()): L {
    Collections.shuffle(this, rand)
    return this
}

fun <M: MutableMap<K, MutableList<V>>, K,V> M.push(k: K, v: V) {
    this.computeIfAbsent(k, { ArrayList<V>() }).add(v)
}

fun <N : Number> List<N>.mean(): Double {
    if (this.isEmpty()) return 0.0
    if (this.size == 1) return this[0].toDouble()
    return this.sumByDouble { it.toDouble() } / this.size.toDouble()
}

fun <N : Number> List<N>.computeStats(): StreamingStats {
    val out = StreamingStats()
    for (x in this) {
        out.push(x.toDouble())
    }
    return out
}

fun <N : Number> Sequence<N>.computeStats(): StreamingStats {
    val out = StreamingStats()
    for (x in this) {
        out.push(x.toDouble())
    }
    return out
}


inline fun <T> List<T>.meanByDouble(mapper: (T)->Double): Double {
    if (this.isEmpty()) return 0.0
    if (this.size == 1) return mapper(this[0])
    return this.sumByDouble { mapper(it) } / this.size.toDouble()
}

inline fun <T> List<T>.forEachSeqPair(fn: (T,T)->Unit) {
    (0 until this.size-1).forEach { i ->
        fn(this[i], this[i+1])
    }
}
inline fun <T,R> List<T>.mapEachSeqPair(fn: (T,T)->R): List<R> {
    return (0 until this.size-1).map { i ->
        fn(this[i], this[i+1])
    }
}
inline fun <T> List<T>.forAllPairs(fn: (T,T)->Unit) {
    (0 until this.size-1).forEach { i ->
        (i until this.size).forEach { j ->
            fn(this[i], this[j])
        }
    }
}
fun <T> List<T>.computeEntropy(): Double {
    val counts = HashMap<T, Int>(this.size/2)
    val length = this.size.toDouble()
    forEach { w -> counts.incr(w, 1) }
    val sum = counts.values.sumByDouble { tf ->
        val p = tf / length
        p * Math.log(p)
    }
    return -sum
}

fun <T> Map<T, Double>.normalize(): Map<T, Double> {
    val norm = this.values.sum()
    return this.mapValues { (_,v) -> v/norm }
}
fun List<Double>.normalize(): List<Double> {
    val norm = this.sum()
    return this.map { v -> v/norm }
}

fun <T> Collection<T>.pfor(doFn: (T)->Unit) {
    this.parallelStream().forEach(doFn)
}
fun <T> Collection<T>.pforIndividual(doFn: (T)->Unit) {
    val pool = java.util.concurrent.ForkJoinPool.commonPool()!!
    this.forEach { input -> pool.submit({ doFn(input) }) }
    while (!pool.awaitQuiescence(1, TimeUnit.SECONDS)) {
        // wait for jobs.
    }
}
fun <I,O> Collection<I>.pmap(mapper: (I)->O): List<O> {
    return this.parallelStream().map { mapper(it) }.toList()
}
fun <I,O> Collection<I>.pmapIndividual(pool: ForkJoinPool = ForkJoinPool.commonPool(), mapper: (I)->O): List<O> {
    val output = Vector<O>()
    this.forEach { input ->
        pool.submit({
            output.add(mapper(input))
        })
    }
    while (!pool.awaitQuiescence(1, TimeUnit.SECONDS)) {
        // wait for jobs.
    }
    return output.toList()
}

fun <T> List<T>.findIndex(x: T): Int? {
    val pos = this.indexOf(x)
    if (pos >= 0) return pos
    return null
}

inline fun <T> try_or_empty(fn: ()->Collection<T>): Collection<T> {
    try {
        return fn()
    } catch (e: Exception) {
        return emptyList<T>()
    }
}

