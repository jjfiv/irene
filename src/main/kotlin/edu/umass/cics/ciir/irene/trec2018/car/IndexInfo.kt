package edu.umass.cics.ciir.irene.trec2018.car

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.galago.getStr
import org.lemurproject.galago.utility.Parameters
import java.io.File

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args);
    val params = IndexParams().apply {
        withPath(File(argp.getStr("input")))
    }
    IreneIndex(params).use { index ->
        val stats: CountStats = index.env.fieldStats(argp.get("field", params.defaultField))
        println(stats);
    }
}