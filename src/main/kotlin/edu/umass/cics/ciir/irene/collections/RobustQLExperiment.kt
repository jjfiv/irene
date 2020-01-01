package edu.umass.cics.ciir.irene.collections

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.galago.toQueryResults
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.BagOfWords
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import edu.umass.cics.ciir.irene.utils.*
import gnu.trove.map.hash.TObjectDoubleHashMap
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.SimpleEvalDoc
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.lists.Ranked
import java.io.File

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val queries: Map<String, String> = File(argp.get("queryFile", "/Users/jfoley/code/queries/robust04/rob04.titles.tsv")).smartLines { lines ->
        lines.associate { line ->
            val row =  line.trim().split("\t")
            Pair(row[0], row[1])
        }
    }

    val qlOutput = File("robust.clrm3.trecrun").smartPrinter()

    IndexParams().apply {
        withPath(File(argp.get("index", "robust04.irene")))
    }.openReader().use { index ->
        index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)

        for ((qid, title) in queries) {
            val titleTerms = index.tokenize(title)
            println("${qid} ${titleTerms.joinToString(separator=" ")}")

            val (time, scores) = timed {
                val ql = QueryLikelihood(titleTerms);
                val cl = WhitelistMatchExpr(docIdentifiers=index.search(ql, 1000).scoreDocs.map { it.doc })
                val rm3 = RelevanceExpansionQ(index.env, ql) ?: error("No expansion for $qid")
                val clrm3 = RequireExpr(cl, rm3)
                index.search(clrm3, 1000)
            }
            System.out.printf("\tRM3 Time: %1.3fs total=%d\n", time, scores.totalHits)
            scores.toQueryResults(index, qid = qid).outputTrecrun(qlOutput, "clrm3")
        }
    }

    qlOutput.close()

}



object RobustVariationExperiment {
    @JvmStatic
    fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val titles: Map<String, String> = File(argp.get("queryFile", "/Users/jfoley/code/queries/robust04/rob04.titles.tsv")).smartLines { lines ->
            lines.associate { line ->
                val row =  line.trim().split("\t")
                Pair(row[0], row[1])
            }
        }

        val queries = titles.keys.sorted()
        val trecRunOutput = File("robust.rm3_expr.trecrun").smartPrinter()

        IndexParams().apply {
            withPath(File(argp.get("index", "robust04.irene")))
        }.openReader().use { index ->
            index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)
            index.env.estimateStats = "min"

            for (qid in queries) {
                if (qid !in titles) {
                    println("Skipping: ${qid}")
                    continue
                }
                val title = titles[qid]!!


                val (time, qres) = timed {
                    val rm3 = RM3Expr(QueryLikelihood(index.tokenize(title)), fbDocs=50)
                    index.search(rm3, 1000).toQueryResults(index, qid)
                }
                qres.outputTrecrun(trecRunOutput, "rm3_expr")

                System.out.printf("${qid}\tRM3_Expr Time: %1.3fs total=%d\n", time, qres.size)
            }
        }

        trecRunOutput.close()

    }
}

object RobustRMTerms {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val titles: Map<String, String> = File(argp.get("queryFile", "/Users/jfoley/code/queries/robust04/rob04.titles.tsv")).smartLines { lines ->
            lines.associate { line ->
                val row = line.trim().split("\t")
                Pair(row[0], row[1])
            }
        }

        val outJSON = Parameters.create();

        IndexParams().apply {
            withPath(File(argp.get("index", "robust04.irene")))
        }.openReader().use { index ->
            index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)
            index.env.estimateStats = "min"

            for ((qid, title) in titles) {
                println("${qid} ${title}")
                val rm = ComputeRelevanceModel(index.env, QueryLikelihood(index.tokenize(title)))
                val termWeights = Parameters.wrap(rm!!.toTerms(1000).associate { Pair(it.term, it.score)})
                outJSON.put(qid, termWeights)
            }
        }

        File("robust.rmTerms.json").writeText(outJSON.toString())
    }
}

object RobustWeightedVariants {
    @JvmStatic
    fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val qdata = Parameters.parseFile(File(argp.get("variantFile", "data/robust_short_queries_roberta_base_widened.json")))
        val titles: Map<String, String> = File(argp.get("queryFile", "/Users/jfoley/code/queries/robust04/rob04.titles.tsv")).smartLines { lines ->
            lines.associate { line ->
                val row =  line.trim().split("\t")
                Pair(row[0], row[1])
            }
        }

        val variantLimit = argp.get("variantLimit", 3);

        val queries = qdata.keys.sorted()
        val sysName = "rm3_roberta_variants_widened.lim${variantLimit}"
        val trecRunOutput = File(argp.get("output", "data/robust.$sysName.trecrun")).smartPrinter()

        IndexParams().apply {
            withPath(File(argp.get("index", "robust04.irene")))
        }.openReader().use { index ->
            index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)
            index.env.estimateStats = "min"

            val msg = CountingDebouncer(total=queries.size.toLong());
            for (qid in queries) {
                if (qid !in titles) {
                    println("Skipping: ${qid}")
                    continue
                }
                val title = titles[qid]!!

                val (time, qres) = timed {
                    // per-query, assume we're tokenizing the same documents over and over:
                    val cache = HashMap<Int, BagOfWords>()

                    // variants with RM expansion:
                    val variations = HashMap<String, QExpr>()
                    val pair = qdata.getAsList(qid, String::class.java)
                    val varText = pair[0].split("\t").map {it.trim()}.filter { it.isNotBlank() }
                    val varWeights = pair[1].split("\t").filter { it.isNotBlank() }.map { it.toDouble() }

                    for ((text, weight) in varText.zip(varWeights).take(variantLimit)) {
                        val q = RelevanceExpansionQ(index.env, QueryLikelihood(index.tokenize(text)), cache=cache) ?: continue
                        variations[text] = q.weighted(weight)
                    }
                    if (argp.get("includeOriginal", true)) {
                        RelevanceExpansionQ(index.env, QueryLikelihood(index.tokenize(title)), cache = cache)?.let { q ->
                            variations.put(title, q)
                        }
                    }

                    if (argp.get("earlyFusion", true)) {
                        val sumVarExpr = SumExpr(variations.values.toList())
                        index.search(sumVarExpr, 1000).toQueryResults(index, qid)

                    } else {

                        val results: Map<String, QueryResults> = index.pool(variations, 1000)
                                .mapValues { (_, td) -> td.toQueryResults(index) }

                        val combSum = TObjectDoubleHashMap<String>()
                        for (qres in results.values) {
                            val normScore = qres.map { it.score }.maxmin()
                            val names = qres.map { it.name }
                            for ((name, score) in names.zip(normScore)) {
                                combSum.adjustOrPutValue(name, score, score)
                            }
                        }
                        val docs = ArrayList<SimpleEvalDoc>()
                        combSum.forEachEntry { name, score ->
                            docs.add(SimpleEvalDoc(name, -1, score))
                        }
                        Ranked.setRanksByScore(docs)
                        QueryResults(qid, docs.subList(0, minOf(1000, docs.size)))
                    }
                }
                qres.outputTrecrun(trecRunOutput, sysName)
                msg.incr()
                System.out.printf("${qid}\t$sysName-Variants Time: %1.3fs total=%d\n", time, qres.size)
                println(msg.now())
            }
        }

        trecRunOutput.close()

    }
}