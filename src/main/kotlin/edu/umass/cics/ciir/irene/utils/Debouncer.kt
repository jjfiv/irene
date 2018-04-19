package edu.umass.cics.ciir.irene.utils

import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author jfoley.
 */
class Debouncer
/**
 * @param delay the minimum number of milliseconds between actions.
 */
@JvmOverloads constructor(val delay: Long = 1000) {
    val startTime: Long = System.currentTimeMillis()
    var lastTime: Long = startTime - delay

    val now: Long get() = System.currentTimeMillis()
    val spentTimePretty: String get() = prettyTimeOfMillis(System.currentTimeMillis() - startTime)

    fun ready(): Boolean {
        val now = System.currentTimeMillis()
        if (now - lastTime >= delay) {
            lastTime = now
            return true
        }
        return false
    }

    fun estimate(currentItem: Long, totalItems: Long): RateEstimate {
        return RateEstimate(System.currentTimeMillis() - startTime, currentItem, totalItems)
    }
}

fun prettyTimeOfMillis(millis: Long): String {
    val msg = StringBuilder()
    var dur = Duration.ofMillis(millis)!!
    val days = dur.toDays()
    if (days > 0) {
        dur = dur.minusDays(days)
        msg.append(days).append(" days")
    }
    val hours = dur.toHours()
    if (hours > 0) {
        dur = dur.minusHours(hours)
        if (msg.length > 0) msg.append(", ")
        msg.append(hours).append(" hours")
    }
    val minutes = dur.toMinutes()
    if (minutes > 0) {
        if (msg.length > 0) msg.append(", ")
        msg.append(minutes).append(" minutes")
    }
    dur = dur.minusMinutes(minutes)
    val seconds = dur.toMillis() / 1e3
    if (msg.length > 0) msg.append(", ")
    msg.append(String.format("%1.3f", seconds)).append(" seconds")
    return msg.toString()
}

class CountingDebouncer(val total: Long, delay: Long = 1000) {
    val count = AtomicLong(0)
    val msg = Debouncer(delay)

    fun incr(amt: Long=1L): RateEstimate? {
        val x = count.addAndGet(amt)
        synchronized(msg) {
            return if (msg.ready()) {
                msg.estimate(x, total)
            } else null
        }
    }

    fun final(): RateEstimate {
        val x = count.get()
        return msg.estimate(x, x)
    }
}


data class RateEstimate(val time: Long, val itemsComplete: Long, val totalItems: Long) {
    /** fraction of job complete  */
    val fraction: Double = itemsComplete / totalItems.toDouble()
    /** items/ms  */
    val rate: Double = itemsComplete / time.toDouble()
    /** estimated time remaining (ms)  */
    val remaining: Double = (totalItems - itemsComplete) / rate

    fun itemsPerSecond(): Double = rate * 1000.0
    fun percentComplete(): Double = fraction * 100.0

    override fun toString(): String =
            String.format("%d/%d items, %4.1f items/s [%s left]; [%s spent], %2.1f%% complete.", itemsComplete, totalItems, itemsPerSecond(), prettyTimeOfMillis(remaining.toLong()), prettyTimeOfMillis(time), percentComplete())
}


inline fun <R> timed(x: ()->R): Pair<Double, R> {
    val start = System.nanoTime()
    val result = x()
    val end = System.nanoTime()
    return Pair((end-start)/ 1e9, result)
}