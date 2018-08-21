package edu.umass.cics.ciir.irene.trec2018.core

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.galago.toQueryResults
import edu.umass.cics.ciir.irene.lang.BM25Expr
import edu.umass.cics.ciir.irene.lang.QueryLikelihood
import edu.umass.cics.ciir.irene.lang.SequentialDependenceModel
import edu.umass.cics.ciir.irene.lang.SmallerCountExpr
import edu.umass.cics.ciir.irene.trec2018.defaultWapoIndexPath
import edu.umass.cics.ciir.irene.utils.smartPrinter
import edu.umass.cics.ciir.irene.utils.timed
import org.lemurproject.galago.utility.Parameters
import java.io.File


fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val queryFile = argp.get("queryFile", listOf(System.getenv("HOME"), "code", "queries", "trec_core", "2018-test-topics.txt").joinToString(separator= File.separator))
    val queries = LoadTrecCoreQueries(queryFile)

    val qlOutput = File("umass_ql.trecrun.gz").smartPrinter()
    val sdmOutput = File("umass_sdm.trecrun.gz").smartPrinter()
    val bsdmOutput = File("umass_bsdm.trecrun.gz").smartPrinter()
    val cbsdmOutput = File("umass_cbsdm.trecrun.gz").smartPrinter()

    IndexParams().apply {
        withPath(File(argp.get("index", defaultWapoIndexPath)))
    }.openReader().use { index ->
        index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)

        for (q in queries.values) {
            val titleTerms = index.tokenize(q.title)
            println("${q.qid} ${titleTerms.joinToString(separator=" ")}")
            val ql = QueryLikelihood(titleTerms)
            val sdmQ = SequentialDependenceModel(titleTerms, uniW=0.8, odW=0.15, uwW=0.05)
            val bm25SDMQ = SequentialDependenceModel(titleTerms, uniW=0.8, odW=0.15, uwW=0.05, makeScorer = { BM25Expr(it) })
            val sdmCheapQ = SequentialDependenceModel(titleTerms, uniW=0.75, odW=0.15, uwW=0.05,
                    stopwords = inqueryStop,
                    uwExpr = {children, _ -> SmallerCountExpr(children)},
                    fullProx = 0.05,
                    makeScorer = { BM25Expr(it) })

            index.env.estimateStats = null
            synchronized(index.env) {
                val (time, scores) = timed { index.search(ql, 10000) }
                System.out.printf("\tQL Time: %1.3fs total=%d\n", time, scores.totalHits)
                scores.toQueryResults(index, qid = q.qid).outputTrecrun(qlOutput, "umass_ql")

            }
            synchronized(index.env) {
                val (time, scores) = timed { index.search(sdmQ, 10000) }
                System.out.printf("\tSDM Time: %1.3fs total=%d\n", time, scores.totalHits)
                scores.toQueryResults(index, qid = q.qid).outputTrecrun(sdmOutput, "umass_sdm")
            }

            synchronized(index.env) {
                val (time, scores) = timed { index.search(bm25SDMQ, 10000) }
                System.out.printf("\tB-SDM Time: %1.3fs total=%d\n", time, scores.totalHits)
                scores.toQueryResults(index, qid = q.qid).outputTrecrun(bsdmOutput, "umass_bsdm")
            }

            index.env.estimateStats = "min"
            index.env.optimizeBM25 = true
            synchronized(index.env) {
                val (time, scores) = timed { index.search(sdmCheapQ, 10000) }
                System.out.printf("\tCB-SDM Time: %1.3fs total=%d\n", time, scores.totalHits)
                scores.toQueryResults(index, qid = q.qid).outputTrecrun(cbsdmOutput, "umass_cbsdm")
            }
        }
    }

    qlOutput.close()
    sdmOutput.close()
    bsdmOutput.close()
    cbsdmOutput.close()
}