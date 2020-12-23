package edu.umass.cics.ciir.irene.utils

import java.util.*

/**
 * @author jfoley
 */
class ReservoirSampler<T>(val numSamples: Int, val rand: Random = Random()) : AbstractList<T>() {
    val backing = ArrayList<T>(numSamples)
    var totalOffered = 0
    inline fun lazyProcess(getIfSampled: ()->T) {
        totalOffered++
        if (backing.size < numSamples) {
            backing.add(getIfSampled())
            return
        }

        // find position in virtual array, and store iff part of our sub-sampled view:
        val position = rand.nextInt(totalOffered)
        if (position >= numSamples) return
        backing.set(position, getIfSampled())
    }
    fun process(element: T) {
        lazyProcess { element }
    }

    override fun clear() {
        backing.clear()
        totalOffered = 0
    }
    override fun add(element: T): Boolean {
        process(element)
        return true
    }
    override fun get(index: Int): T = backing[index]
    override val size: Int
        get() = numSamples
}

fun <T> Iterable<T>.sample(numSamples: Int, rand: Random = Random()): ReservoirSampler<T> {
    val sampler = ReservoirSampler<T>(numSamples, rand)
    for (e in this) { sampler.process(e) }
    return sampler
}
fun <T> Iterator<T>.sample(numSamples: Int, rand: Random = Random()): ReservoirSampler<T> {
    val sampler = ReservoirSampler<T>(numSamples, rand)
    while(this.hasNext()) {
        sampler.process(this.next())
    }
    return sampler
}
