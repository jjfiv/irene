package edu.umass.cics.ciir.irene.trec2018.core

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.galago.toQueryResults
import edu.umass.cics.ciir.irene.lang.QueryLikelihood
import edu.umass.cics.ciir.irene.trec2018.defaultWapoIndexPath
import edu.umass.cics.ciir.irene.utils.smartPrinter
import edu.umass.cics.ciir.irene.utils.timed
import org.lemurproject.galago.utility.Parameters
import java.io.File

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val queryFile = argp.get("queryFile", "${System.getenv("HOME")}/code/queries/trec_core/2018-test-topics.txt")
    val queries = LoadTrecCoreQueries(queryFile)
    val qlOutput = File("umass_ql.trecrun.gz").smartPrinter()

    IndexParams().apply {
        withPath(File(argp.get("index", defaultWapoIndexPath)))
    }.openReader().use { index ->
        index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)

        for (q in queries.values) {
            val titleTerms = index.tokenize(q.title)
            println("${q.qid} ${titleTerms.joinToString(separator=" ")}")
            val ql = QueryLikelihood(titleTerms)
            val (time, scores) = timed { index.search(ql, 10000) }
            System.out.printf("\tQL Time: %1.3fs total=%d\n", time, scores.totalHits)
            scores.toQueryResults(index, qid = q.qid).outputTrecrun(qlOutput, "umass_ql")
        }
    }

    qlOutput.close()
}