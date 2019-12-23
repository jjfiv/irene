package edu.umass.cics.ciir.irene.collections

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.galago.toQueryResults
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.BagOfWords
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import edu.umass.cics.ciir.irene.utils.smartLines
import edu.umass.cics.ciir.irene.utils.smartPrinter
import edu.umass.cics.ciir.irene.utils.timed
import gnu.trove.map.hash.TObjectDoubleHashMap
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters
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
                val rm3 = RelevanceExpansionQ(index, ql)
                val clrm3 = RequireExpr(cl, rm3)
                index.search(clrm3, 1000)
            }
            System.out.printf("\tRM3 Time: %1.3fs total=%d\n", time, scores.totalHits)
            scores.toQueryResults(index, qid = qid).outputTrecrun(qlOutput, "clrm3")
        }
    }

    qlOutput.close()

}

fun RelevanceExpansionQ(index: IreneIndex, firstPass: QExpr, depth: Int=50, origWeight: Double = 0.3, numTerms: Int=100, expansionField: String?=null, stopwords: Set<String> = inqueryStop, cache: HashMap<Int, BagOfWords> = HashMap()): QExpr {
    val field = expansionField ?: index.env.defaultField
    val firstPassResults = index.search(firstPass, depth).scoreDocs
    val norm = MathUtils.logSumExp(firstPassResults.map { it.score.toDouble() }.toDoubleArray())
    val weights = TObjectDoubleHashMap<String>()

    for (sdoc in firstPassResults) {
        // pull document, tokenize, and count up if unseen
        val docWords = cache.computeIfAbsent(sdoc.doc) { num ->
            BagOfWords(index.terms(num, field))
        }
        val prior = Math.exp(sdoc.score - norm)
        docWords.counts.forEachEntry { term, count ->
            if (stopwords.contains(term)) {
                return@forEachEntry true
            }
            val p = prior * (count / docWords.length)
            weights.adjustOrPutValue(term, p, p)
            true
        }
    }
    val expQ = RelevanceModel(weights, expansionField).toQExpr(numTerms)
    return SumExpr(firstPass.weighted(origWeight), expQ.weighted(1.0 - origWeight))
}

object RobustVariationExperiment {
    @JvmStatic
    fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val qdata = Parameters.parseFile(File("variations_dict.json"))
        val titles: Map<String, String> = File(argp.get("queryFile", "/Users/jfoley/code/queries/robust04/rob04.titles.tsv")).smartLines { lines ->
            lines.associate { line ->
                val row =  line.trim().split("\t")
                Pair(row[0], row[1])
            }
        }
        println(qdata);


        val queries = qdata.keys.sorted()
        val trecRunOutput = File("robust.rm3_variants_combsum.trecrun").smartPrinter()

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
                    // per-query, assume we're tokenizing the same documents over and over:
                    val cache = HashMap<Int, BagOfWords>()

                    // variants with RM expansion:
                    val variations: HashMap<String, QExpr> = qdata.getStr(qid).split('\t').associateTo(HashMap()) { variant ->
                        Pair(variant, RelevanceExpansionQ(index, QueryLikelihood(index.tokenize(variant)), cache=cache))
                    }
                    variations.put(title, RelevanceExpansionQ(index, QueryLikelihood(index.tokenize(title)), cache=cache))

                    val sumVarExpr = SumExpr(variations.values.toList())
                    /*
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
                    */
                    index.search(sumVarExpr, 1000).toQueryResults(index, qid)
                }
                qres.outputTrecrun(trecRunOutput, "sdm_variants_rm3")

                System.out.printf("${qid}\tSDM-Variants Time: %1.3fs total=%d\n", time, qres.size)
            }
        }

        trecRunOutput.close()

    }
}