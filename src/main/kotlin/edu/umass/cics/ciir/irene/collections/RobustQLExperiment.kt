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
                val rm3 = RelevanceExpansionQ(index, ql) ?: error("No expansion for $qid")
                val clrm3 = RequireExpr(cl, rm3)
                index.search(clrm3, 1000)
            }
            System.out.printf("\tRM3 Time: %1.3fs total=%d\n", time, scores.totalHits)
            scores.toQueryResults(index, qid = qid).outputTrecrun(qlOutput, "clrm3")
        }
    }

    qlOutput.close()

}

fun ComputeRelevanceModel(index: IreneIndex, firstPass: QExpr, depth: Int=50, expansionField: String?=null, stopwords: Set<String> = inqueryStop, cache: HashMap<Int, BagOfWords> = HashMap()): RelevanceModel? {
    val field = expansionField ?: index.env.defaultField
    val firstPassResults = index.search(firstPass, depth).scoreDocs
    if (firstPassResults.isEmpty()) {
        return null
    }
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
    return RelevanceModel(weights, expansionField)
}

fun RelevanceExpansionQ(index: IreneIndex, firstPass: QExpr, depth: Int=50, origWeight: Double = 0.3, numTerms: Int=100, expansionField: String?=null, stopwords: Set<String> = inqueryStop, cache: HashMap<Int, BagOfWords> = HashMap()): QExpr? {
    val rm = ComputeRelevanceModel(index, firstPass, depth, expansionField, stopwords, cache) ?: return null
    val expQ = rm.toQExpr(numTerms)
    return SumExpr(firstPass.weighted(origWeight), expQ.weighted(1.0 - origWeight))
}

object RobustVariationExperiment {
    @JvmStatic
    fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val qdata = Parameters.parseFile(File("human_query_variations.json"))
        val titles: Map<String, String> = File(argp.get("queryFile", "/Users/jfoley/code/queries/robust04/rob04.titles.tsv")).smartLines { lines ->
            lines.associate { line ->
                val row =  line.trim().split("\t")
                Pair(row[0], row[1])
            }
        }
        println(qdata);


        val queries = qdata.keys.sorted()
        val trecRunOutput = File("robust.rm3_human_variants.trecrun").smartPrinter()

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
                    val variations = HashMap<String, QExpr>()
                    for (variant in qdata.getStr(qid).trim().split('\t')) {
                        if (variant.isBlank()) {
                            continue
                        }
                        val q = RelevanceExpansionQ(index, QueryLikelihood(index.tokenize(variant)), cache=cache) ?: continue
                        variations[variant] = q
                    }
                    RelevanceExpansionQ(index, QueryLikelihood(index.tokenize(title)), cache=cache)?.let { q ->
                        variations.put(title, q)
                    }

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
                qres.outputTrecrun(trecRunOutput, "human_variants_rm3")

                System.out.printf("${qid}\tHuman-Variants-RM3 Time: %1.3fs total=%d\n", time, qres.size)
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
                val rm = ComputeRelevanceModel(index, QueryLikelihood(index.tokenize(title)))
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
        val qdata = Parameters.parseFile(File("data/robust_short_queries_roberta_base.json"))
        val titles: Map<String, String> = File(argp.get("queryFile", "/Users/jfoley/code/queries/robust04/rob04.titles.tsv")).smartLines { lines ->
            lines.associate { line ->
                val row =  line.trim().split("\t")
                Pair(row[0], row[1])
            }
        }
        println(qdata);


        val queries = qdata.keys.sorted()
        val sysName = "rm3_roberta_variants"
        val trecRunOutput = File("data/robust.$sysName.trecrun").smartPrinter()

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
                    val variations = HashMap<String, QExpr>()
                    val pair = qdata.getAsList(qid, String::class.java)
                    val varText = pair[0].split("\t").map {it.trim()}.filter { it.isNotBlank() }
                    val varWeights = pair[1].split("\t").filter { it.isNotBlank() }.map { it.toDouble() }

                    for ((text, weight) in varText.zip(varWeights)) {
                        val q = RelevanceExpansionQ(index, QueryLikelihood(index.tokenize(text)), cache=cache) ?: continue
                        variations[text] = q.weighted(weight)
                    }
                    RelevanceExpansionQ(index, QueryLikelihood(index.tokenize(title)), cache=cache)?.let { q ->
                        variations.put(title, q)
                    }

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
                qres.outputTrecrun(trecRunOutput, sysName)

                System.out.printf("${qid}\t$sysName-Variants Time: %1.3fs total=%d\n", time, qres.size)
            }
        }

        trecRunOutput.close()

    }
}