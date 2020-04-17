package edu.umass.cics.ciir.irene.collections

import edu.umass.cics.ciir.irene.docs
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.smartPrint
import org.apache.lucene.benchmark.byTask.feeds.TrecContentSource
import org.apache.lucene.benchmark.byTask.utils.Config
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.*

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val input = File(argp.get("input", "robust"))
    assert(input.exists())

    val dataset = TrecContentSource()
    dataset.config = Config(Properties().apply {
        set("content.source.excludeIteration", "true")
        set("content.source.forever", "false")
        set("tests.verbose", "false")
        set("docs.dir", input.absolutePath)
    })

    val msg = CountingDebouncer(528_096L)
    File("robust.jsonl.gz").smartPrint { writer ->
        dataset.docs().forEach { trec ->
            val json = Parameters.create();
            json["id"] = trec.name
            json["title"] = trec.title
            json["body"] = trec.body;
            writer.println(json)
            msg.incr()?.let { update ->
                println(update)
            }
        }

    }

    println("---")
}
