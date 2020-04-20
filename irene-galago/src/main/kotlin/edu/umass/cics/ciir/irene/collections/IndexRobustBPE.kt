package edu.umass.cics.ciir.irene.collections

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.smartDoLines
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.lemurproject.galago.utility.Parameters
import java.io.File

val RobustBPEParams = IndexParams().apply {
    defaultField = "body"
    withPath(File("robust04.bpe.irene"))
    withAnalyzer("title", WhitespaceAnalyzer())
    withAnalyzer("body", WhitespaceAnalyzer())
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val input = File(argp.get("input", "robust"))
    assert(input.exists())



    val msg = CountingDebouncer(528_096L)
    IreneIndexer(RobustBPEParams.create()).use { writer ->
        File("robust.bpe.jsonl.gz").smartDoLines { line ->
            val docP = Parameters.parseString(line)
            val title = docP.getAsList("title").joinToString(separator=" ")
            val body = docP.getAsList("body").joinToString(separator=" ")
            writer.doc {
                setId(docP.getStr("id"))
                setTextField("title", title)
                setTextField(params.defaultField, body)
            }

            msg.incr()?.let { update ->
                println(update)
            }
        }
    }
    println("---")
}
