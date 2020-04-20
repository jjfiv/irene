package edu.umass.cics.ciir.irene

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import edu.umass.cics.ciir.irene.indexing.IreneIndexer
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.lucene.CalculateStatistics
import edu.umass.cics.ciir.irene.scoring.IreneQueryModel
import edu.umass.cics.ciir.irene.utils.ReservoirSampler
import org.apache.lucene.benchmark.byTask.feeds.DocData
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException
import org.apache.lucene.benchmark.byTask.feeds.TrecContentSource
import org.apache.lucene.index.*
import org.apache.lucene.search.*
import org.lemurproject.galago.utility.Parameters
import org.roaringbitmap.RoaringBitmap
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinTask

/**
 *
 * @author jfoley.
 */

fun IndexParams.openReader() = IreneIndex(this)
fun IndexParams.openWriter() = IreneIndexer(this)

fun LDoc.toParameters(): Parameters {
    val output = Parameters.create()
    fields.forEach { field ->
        val name = field.name()!!
        output.putIfNotNull(name, field.stringValue())
        output.putIfNotNull(name, field.numericValue())
    }
    return output
}




fun TrecContentSource.docs(): Sequence<DocData> = sequence {
    while(true) {
        val doc = DocData()
        try {
            getNextDocData(doc)
            yield(doc)
        } catch (e: NoMoreDataException) {
            break
        } catch (e: Throwable) {
            e.printStackTrace(System.err)
        }
    }
}

