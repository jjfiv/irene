package edu.umass.cics.ciir.irene.lang

import org.apache.lucene.search.CollectionStatistics
import org.apache.lucene.search.TermStatistics

/**
 * CountStats represents the countable statistics for a term or expression.
 * @author jfoley.
 */
data class CountStats(var source: String, var cf: Long, var df: Long, var cl: Long, var dc: Long) {
    constructor(text: String, field: String): this("$field:$text", 0,0,0,0)
    constructor(text: String, termStats: TermStatistics?, cstats: CollectionStatistics) : this("${cstats.field()}:$text",
            cf=termStats?.totalTermFreq() ?: 0,
            df=termStats?.docFreq() ?: 0,
            cl=cstats.sumTotalTermFreq(),
            dc=cstats.docCount())

    fun avgDL() = cl.toDouble() / dc.toDouble();
    fun countProbability() = cf.toDouble() / cl.toDouble()
    fun nonzeroCountProbability() = Math.max(0.5,cf.toDouble()) / cl.toDouble()
    fun binaryProbability() = df.toDouble() / dc.toDouble()
    operator fun plusAssign(rhs: CountStats?) {
        if (rhs != null) {
            cl += rhs.cl
            df += rhs.df
            cf += rhs.cf
            dc += rhs.dc
        }
    }
}
