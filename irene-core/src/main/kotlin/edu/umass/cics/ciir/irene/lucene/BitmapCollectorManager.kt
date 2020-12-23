package edu.umass.cics.ciir.irene.lucene

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.Collector
import org.apache.lucene.search.CollectorManager
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.search.Scorer
import org.roaringbitmap.RoaringBitmap

/**
 * This class collects all the matching results from a query.
 * @author jfoley.
 */
class BitmapCollectorManager: CollectorManager<BitmapCollector, RoaringBitmap> {
    override fun newCollector(): BitmapCollector = BitmapCollector()
    override fun reduce(collectors: Collection<BitmapCollector>): RoaringBitmap {
        val output = RoaringBitmap()
        for (c in collectors) {
            output.or(c.results)
        }
        return output
    }
}

class BitmapCollector : Collector {
    val results = RoaringBitmap()
    override fun needsScores(): Boolean = false
    override fun getLeafCollector(context: LeafReaderContext): LeafCollector {
        val docBase = context.docBase
        return object : LeafCollector {
            override fun setScorer(scorer: Scorer?) {}
            override fun collect(doc: Int) {
                results.add(doc + docBase)
            }
        }
    }
}
