package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.indexing.IndexParams
import edu.umass.cics.ciir.irene.indexing.IreneIndexer
import org.apache.lucene.benchmark.byTask.feeds.DocData
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException
import org.apache.lucene.benchmark.byTask.feeds.TrecContentSource
import org.lemurproject.galago.utility.Parameters

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

