package edu.umass.cics.ciir.irene.lucene

import edu.umass.cics.ciir.irene.lang.CountStats
import edu.umass.cics.ciir.irene.scoring.IreneQueryModel
import org.apache.lucene.index.Term
import org.apache.lucene.index.TermContext
import org.apache.lucene.search.IndexSearcher

/**
 *
 * @author jfoley.
 */
object CalculateStatistics {
    fun lookupTermStatistics(searcher: IndexSearcher, term: Term): CountStats? {
        val cstats = searcher.collectionStatistics(term.field())
        val ctx = TermContext.build(searcher.topReaderContext, term) ?: return null
        val termStats = searcher.termStatistics(term, ctx) ?: return null
        return CountStats("term:$term", termStats, cstats)
    }

    fun fieldStats(searcher: IndexSearcher, field: String): CountStats? {
        val cstats = searcher.collectionStatistics(field) ?: return null
        return CountStats("field:$field", null, cstats)
    }

    inline fun computeQueryStats(searcher: IndexSearcher, query: IreneQueryModel, cachedFieldStats: (String)->(CountStats?)): CountStats {
        //println("Computing: ${query.exec}")
        //query.movement = query.exec
        val fields = query.exec.getStatsFields()

        val fieldBasedStats = CountStats("expr:${query.exec}", fields.first())
        fields.forEach { field ->
            val fstats = cachedFieldStats(field) ?: error("Field: ``$field'' does not exist in index.")
            fieldBasedStats.dc = maxOf(fstats.dc, fieldBasedStats.dc)
            fieldBasedStats.cl += fstats.cl
        }
        return searcher.search(query, CountStatsCollectorManager(fieldBasedStats, fields.first()))
    }
}
