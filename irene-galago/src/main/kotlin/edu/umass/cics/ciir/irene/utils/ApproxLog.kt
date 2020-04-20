package edu.umass.cics.ciir.irene.utils

import java.util.concurrent.ThreadLocalRandom
import kotlin.streams.asSequence

/**
 * Based on polynomials in BSD-licensed fastlog.h
 * [Github](https://github.com/romeric/fastapprox/blob/master/fastapprox/src/fastlog.h)
 */
object ApproxLog {
    fun fast_log2(vxd: Double): Double {
        val vxf = vxd.toFloat()
        val vxi = java.lang.Float.floatToRawIntBits(vxf)
        val mxi = (vxi and 0x007fffff) or (0x3f000000)
        val mxf = java.lang.Float.intBitsToFloat(mxi)
        val y = vxi.toDouble() * 1.1920928955078125e-7;

        return y - (124.22551499
                - 1.498030302 * mxf
                - 1.72587999 / (0.3520887068 + mxf))

    }
    fun fast_log(vxd: Double) = fast_log2(vxd) * 0.69314718;
    fun faster_log2(vxd: Double): Double {
        val vxi = java.lang.Float.floatToRawIntBits(vxd.toFloat())
        val y = vxi.toDouble() * 1.1920928955078125e-7
        return y - 126.94269504
    }

    fun faster_log(vxd: Double): Double {
        val vxi = java.lang.Float.floatToRawIntBits(vxd.toFloat())
        val y = vxi.toDouble() * 8.2629582881927490e-8
        return y - 87.989971088
    }

    @JvmStatic fun main(args: Array<String>) {
        // Faster-log is 4x as fast as log after JIT warmup (laptop).
        // Fast-log is only 2x as fast as log after JIT warmup.
        // Experiments on Robust show no loss in error. (note that most dirichlet probabilities are p << 0.25), so this is an extremely restricted range.
        println("faster_log(0.0)=${faster_log(0.0)}")

        (0 until 10).forEach { trial ->
            val data = ThreadLocalRandom.current().doubles(0.0000001, 0.25).asSequence().take(5_000_000).toList().toDoubleArray()

            val (est_time, est_ss) = timed {
                val ss = StreamingStats()
                data.forEach {
                    ss.push(faster_log(it))
                }
                ss
            }
            val (act_time, act_ss) = timed {
                val ss = StreamingStats()
                data.forEach {
                    ss.push(Math.log(it))
                }
                ss
            }

            println("\t%d\tActual: %3.3fs ss=%s".format(trial, act_time, act_ss))
            println("\t%d\tEstimated: %3.3fs ss=%s".format(trial, est_time, est_ss))
        }
    }
}