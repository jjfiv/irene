package edu.umass.cics.ciir.irene.lucene

import edu.umass.cics.ciir.irene.utils.ReservoirSampler
import org.apache.lucene.search.CollectorManager
import java.util.*

/**
 *
 * @author jfoley.
 */
// This class samples up to requested results from a query.
class SamplingCollectorManager(val requested: Int, val rand: Random): CollectorManager<BitmapCollector, ReservoirSampler<Int>> {
    override fun newCollector(): BitmapCollector = BitmapCollector()
    override fun reduce(collectors: Collection<BitmapCollector>): ReservoirSampler<Int> {
        val output = ReservoirSampler<Int>(requested, rand)
        for (c in collectors) {
            c.results.forEach { doc ->
                output.add(doc)
            }
        }
        return output
    }
}
