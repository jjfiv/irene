package edu.umass.cics.ciir.irene.lucene

import edu.umass.cics.ciir.irene.IreneWeightedDoc
import edu.umass.cics.ciir.irene.utils.ScoringHeap
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.*

/**
 *
 * @author jfoley.
 */
class TopKCollectorManager(val requested: Int) : CollectorManager<TopKCollectorManager.TopKCollector, TopDocs> {
    override fun reduce(collectors: MutableCollection<TopKCollector>): TopDocs {
        val totalSeen = collectors.map { it.heap.totalSeen }.sum()

        val output = ScoringHeap<IreneWeightedDoc>(requested)
        for (c in collectors) {
            for (d in c.heap.unsortedList) {
                output.offer(d)
            }
        }
        return toTopDocs(output, totalSeen)
    }

    inner class TopKCollector() : Collector {
        val heap = ScoringHeap<IreneWeightedDoc>(requested)
        override fun needsScores(): Boolean = true
        override fun getLeafCollector(context: LeafReaderContext): LeafCollector {
            val docBase = context.docBase
            return object : LeafCollector {
                lateinit var query: Scorer
                override fun setScorer(scorer: Scorer) {
                    query = scorer
                }
                override fun collect(doc: Int) {
                    val score = query.score()
                    heap.offer(score, { IreneWeightedDoc(score, doc+docBase) })
                }
            }
        }
    }

    override fun newCollector() = TopKCollector()
}

fun toTopDocs(heap: ScoringHeap<IreneWeightedDoc>, totalSeen: Long=heap.totalSeen): TopDocs {
    val arr = heap.sorted.map { ScoreDoc(it.doc, it.weight) }.toTypedArray()
    val max = arr.map { it.score }.max() ?: -Float.MAX_VALUE
    return TopDocs(totalSeen, arr, max)
}
