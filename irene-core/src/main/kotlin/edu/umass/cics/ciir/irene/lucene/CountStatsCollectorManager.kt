package edu.umass.cics.ciir.irene.lucene

import edu.umass.cics.ciir.irene.lang.CountStats
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.Collector
import org.apache.lucene.search.CollectorManager
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.search.Scorer

/**
 *
 * @author jfoley.
 */
class CountStatsCollectorManager(val start: CountStats, val field: String) : CollectorManager<CountStatsCollectorManager.CountStatsCollector, CountStats> {
    override fun reduce(collectors: Collection<CountStatsCollector>): CountStats {
        val out = start.copy()
        collectors.forEach {
            out += it.stats
        }
        return out
    }

    class CountStatsCollector(field: String) : Collector {
        val stats = CountStats("tmp:CountStatsCollector", field)
        override fun needsScores(): Boolean = false
        override fun getLeafCollector(context: LeafReaderContext): LeafCollector {
            val docBase = context.docBase
            return object : LeafCollector {
                lateinit var scoreFn: Scorer
                override fun setScorer(scorer: Scorer) {
                    scoreFn = scorer
                }

                override fun collect(doc: Int) {
                    val score = scoreFn.score()
                    val count = score.toInt()
                    assert(score - count < 1e-10, { "Collecting count stats but got float score: ${docBase + doc} -> $score -> $count" })

                    if (count > 0) {
                        stats.cf += count
                        stats.df += 1
                    }
                }
            }
        }
    }

    override fun newCollector(): CountStatsCollector = CountStatsCollector(field)
}
