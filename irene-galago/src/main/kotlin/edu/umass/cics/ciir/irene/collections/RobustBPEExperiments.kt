package edu.umass.cics.ciir.irene.collections

import edu.umass.cics.ciir.irene.galago.toQueryResults
import edu.umass.cics.ciir.irene.lang.SequentialDependenceModel
import edu.umass.cics.ciir.irene.openReader
import edu.umass.cics.ciir.irene.utils.smartLines
import edu.umass.cics.ciir.irene.utils.smartPrinter
import edu.umass.cics.ciir.irene.utils.timed
import org.lemurproject.galago.utility.Parameters
import java.io.File

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val queries: Map<String, String> = File(argp.get("queryFile", "robust.titles.bpe.tsv")).smartLines { lines ->
        lines.associate { line ->
            val row =  line.trim().split("\t")
            Pair(row[0], row[1])
        }
    }

    val qlOutput = File("robust.bpe_sdm.trecrun.gz").smartPrinter()

    RobustBPEParams.openReader().use { index ->
        index.env.config.defaultDirichletMu = index.getAverageDL(index.defaultField)

        for ((qid, title) in queries) {
            val titleTerms = index.tokenize(title)
            println("${qid} ${titleTerms.joinToString(separator=" ")}")
            val ql = SequentialDependenceModel(titleTerms)
            val (time, scores) = timed { index.search(ql, 1000) }
            System.out.printf("\tQL Time: %1.3fs total=%d\n", time, scores.totalHits)
            scores.toQueryResults(index, qid = qid).outputTrecrun(qlOutput, "bpe_ql")
        }
    }

    qlOutput.close()
}