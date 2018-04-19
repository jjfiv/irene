package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.utils.ScoringHeap
import edu.umass.cics.ciir.irene.utils.WeightedForHeap
import edu.umass.cics.ciir.irene.lang.MultiExpr
import edu.umass.cics.ciir.irene.scoring.IreneQueryScorer
import edu.umass.cics.ciir.irene.scoring.MultiEvalNode
import edu.umass.cics.ciir.irene.scoring.ScoringEnv
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.*

/**
 * @author jfoley
 */

data class IreneWeightedDoc(
        override val weight: Float,
        val doc: Int
) : WeightedForHeap {}

fun toTopDocs(heap: ScoringHeap<IreneWeightedDoc>, totalSeen: Long=heap.totalSeen): TopDocs {
    val arr = heap.sorted.map { ScoreDoc(it.doc, it.weight) }.toTypedArray()
    val max = arr.map { it.score }.max() ?: -Float.MAX_VALUE
    return TopDocs(totalSeen, arr, max)
}

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
                    heap.offer(score, { IreneWeightedDoc(score, doc+docBase)})
                }
            }
        }
    }

    override fun newCollector() = TopKCollector()
}

class PoolingCollectorManager(val mq: MultiExpr, val poolSize: Int): CollectorManager<PoolingCollectorManager.PoolingCollector, Map<String, TopDocs>> {
    val names = mq.names

    override fun reduce(collectors: Collection<PoolingCollector>): Map<String, TopDocs> {
        // Collect final top-$poolSize results per inner expression.
        val heaps = names.map { ScoringHeap<IreneWeightedDoc>(poolSize) }
        val matches = names.mapTo(ArrayList()) { 0L }
        for (c in collectors) {
            c.heaps.forEachIndexed { i, heap ->
                matches[i] += heap.totalSeen
                for (d in heap.unsortedList) {
                    heaps[i].offer(d)
                }
            }
        }

        // patch up the "totalHits" part of the heaps:
        val topdocs = heaps.zip(matches).map { (heap, hits) -> toTopDocs(heap, hits) }
        return names.zip(topdocs).associate { it }
    }

    override fun newCollector() = PoolingCollector()

    inner class PoolingCollector : Collector {
        val heaps = names.map { ScoringHeap<IreneWeightedDoc>(poolSize) }

        override fun needsScores(): Boolean = true
        override fun getLeafCollector(context: LeafReaderContext): LeafCollector {
            val docBase = context.docBase
            return object : LeafCollector {
                lateinit var eval: MultiEvalNode
                lateinit var env: ScoringEnv
                override fun setScorer(scorer: Scorer) {
                    val iqs = scorer as IreneQueryScorer
                    eval = iqs.eval as MultiEvalNode
                    env = iqs.env
                }
                override fun collect(doc: Int) {
                    val gdoc = doc + docBase
                    assert(env.doc == doc)
                    // score any matching sub-expressions:
                    eval.children.forEachIndexed { i, node ->
                        if (node.matches(env)) {
                            val score = node.score(env).toFloat()
                            heaps[i].offer(score, { IreneWeightedDoc(score, gdoc) })
                        }
                    }
                }
            }
        }
    }
}