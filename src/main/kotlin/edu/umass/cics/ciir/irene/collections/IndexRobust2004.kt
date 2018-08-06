package edu.umass.cics.ciir.irene.collections

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.docs
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import org.apache.lucene.benchmark.byTask.feeds.TrecContentSource
import org.apache.lucene.benchmark.byTask.utils.Config
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.*

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val input = File(argp.get("input", "robust"))
    assert(input.exists())

    val params = IndexParams().apply {
        create()
        withPath(File(argp.get("index", "robust04.irene")))
    }

    val dataset = TrecContentSource()
    dataset.config = Config(Properties().apply {
        set("content.source.excludeIteration", "true")
        set("content.source.forever", "false")
        set("tests.verbose", "false")
        set("docs.dir", input.absolutePath)
    })

    val msg = CountingDebouncer(528_096L)
    IreneIndexer(params).use { writer ->
        dataset.docs().forEach { trec ->
            writer.doc {
                setId(trec.name)
                setTextField("title", trec.title)
                setTextField(params.defaultField, trec.body)
            }

            msg.incr()?.let { update ->
                println(update)
            }
        }
    }
    println("---")
}
