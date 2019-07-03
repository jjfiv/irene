package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.LDoc
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import edu.umass.cics.ciir.irene.ltr.WeightedItem
import edu.umass.cics.ciir.irene.trec2018.defaultWapoIndexPath
import edu.umass.cics.ciir.irene.utils.forAllPairs
import edu.umass.cics.ciir.irene.utils.smartPrint
import edu.umass.cics.ciir.irene.utils.timed
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.jsoup.Jsoup
import org.jsoup.parser.Parser
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.SimpleEvalDoc
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.lang.Math.abs

data class TrecNewsBGLinkQuery(val qid: String, val docid: String, val url: String)

fun loadBGLinkQueries(path: String): List<TrecNewsBGLinkQuery> {
    val trecXMLDoc = File(path)
    if (!trecXMLDoc.exists()) error("TREC News BG Linking Query file not found at: $path")
    val doc = Jsoup.parse(trecXMLDoc, "UTF-8")
    return doc.select("top").map { query ->
        val qid = query.selectFirst("num").text().substringAfter("Number:").trim()
        val docid = query.selectFirst("docid").text().trim()
        val url = query.selectFirst("url").text().trim()
        TrecNewsBGLinkQuery(qid, docid, url)
    }
}
data class TrecNewsEntity(val id: String, val mention: String, val link: String)
data class TrecNewsEntityLinkQuery(val qid: String, val docid: String, val url: String, val ent: List<TrecNewsEntity>)
fun loadEntityLinkQueries(path: String): List<TrecNewsEntityLinkQuery> {
    val trecXMLDoc = File(path)
    if (!trecXMLDoc.exists()) error("TREC News BG Linking Query file not found at: $path")
    val doc = Jsoup.parse(trecXMLDoc.readText(Charsets.UTF_8), "", Parser.xmlParser())
    return doc.select("top").map { query ->
        val qid = query.selectFirst("num").text().substringAfter("Number:").trim()
        val docid = query.selectFirst("docid").text().trim()
        val url = query.selectFirst("url").text().trim()

        val entities = query.select("entity").map { etag ->
            val id = etag.selectFirst("id").text().trim()
            val mention = etag.selectFirst("mention").text().trim()
            val link = etag.selectFirst("link").text().trim()
            assert(link.startsWith("enwiki:"))
            TrecNewsEntity(id, mention, link)
        }

        TrecNewsEntityLinkQuery(qid, docid, url, entities)
    }
}
data class WapoDocument(
        val id: String,
        val published_date: Long?,
        val url: String,
        val kind: String?,
        val title: String?,
        val author: String?,
        val body: String
)

fun fromLucene(index: IreneIndex, doc: LDoc) = WapoDocument(
        doc.get(index.idFieldName),
        doc.getField("published_date")?.numericValue()?.toLong(),
        doc.get("url"),
        doc.get("kind"),
        doc.get("title"),
        doc.get("author"),
        doc.get(index.defaultField)
)

val BackgroundLinkingDepth = 100

fun documentVectorToQuery(tokens: List<String>,
                          numTerms: Int,
                          stopwords: Set<String> = inqueryStop,
                          targetField: String? = null,
                          windowDetectionSize: Int = 8,
                          scorer: (QExpr) -> QExpr = { DirQLExpr(it) },
                          proxToExpr: (List<QExpr>) -> QExpr = {UnorderedWindowExpr(it)},
                          unigramWeight: Double = 0.8,
                          unigrams: Boolean = false,
                          numBigrams: Int = 20
                      ): QExpr {
    val counts = TObjectIntHashMap<String>()
    val length = tokens.size.toDouble()
    for (t in tokens) {
        if (stopwords.contains(t)) continue
        counts.adjustOrPutValue(t, 1, 1)
    }
    val weights = TObjectDoubleHashMap<String>()
    counts.forEachEntry {term, count ->
        weights.put(term, count / length)
        true
    }
    val rm = RelevanceModel(weights, targetField)
    val terms = rm.toTerms(numTerms).map { it.term }

    // Identify dependencies from document given RM terms and weights:
    val positions = hashMapOf<String, MutableList<Int>>()
    for (i in tokens.indices) {
        terms.find { it == tokens[i] }?.let { found ->
            positions.computeIfAbsent(found, {ArrayList()}).add(i)
        }
    }
    val mins = hashMapOf<Pair<String,String>, Int>()
    positions.keys.toList().forAllPairs { lhs, rhs ->
        if (lhs == rhs) {
            return@forAllPairs
        }
        val lp = positions[lhs]!!
        val rp = positions[rhs]!!

        var min = Integer.MAX_VALUE
        for (li in lp) {
            for (ri in rp) {
                val newMin = abs(ri - li)
                if (min > newMin) {
                    min = newMin
                }
            }
        }
        if (min <= windowDetectionSize) {
            mins.put(Pair(lhs, rhs), min)
        }
    }

    val expansionNodes = mins.keys.map { (lhs, rhs) ->
        val importance = rm.weights[lhs] * rm.weights[rhs]
        WeightedItem(importance, scorer(proxToExpr(listOf(TextExpr(lhs), TextExpr(rhs)))).weighted(importance))
    }.sorted().take(numBigrams)
    val uww = CombineExpr(expansionNodes.map { it.item }, expansionNodes.map { it.score })

    if (unigrams) {
        println("Just unigrams!")
        return rm.toQExpr(numTerms, scorer=scorer)
    } else {
        println("Not unigrams! ${expansionNodes.size} ${expansionNodes.take(10)}")
        return SumExpr(rm.toQExpr(numTerms, scorer=scorer).weighted(unigramWeight), uww.weighted(1.0-unigramWeight))
    }
}

val skipTheseKickers = setOf("Opinions", "Letters to the Editor", "Opinion", "The Post's View")

fun executeBackgroundLinkingQuery(q: TrecNewsBGLinkQuery, index: IreneIndex, docNo: Int, doc: WapoDocument, query: QExpr, includeFuture: Boolean = true): QueryResults {
    val time = doc.published_date ?: 0L
    assert(time >= 0L)
    println("${q.qid}, ${doc.title}\n\t${doc.url}")

    val publishedBeforeExpr = LongLTE(DenseLongField("published_date"), time)
    val finalExpr = if (includeFuture) {
      query.deepCopy()
    } else {
      MustExpr(publishedBeforeExpr, query).deepCopy()
    }

    val (scoring_time, results) = timed { index.search(finalExpr, BackgroundLinkingDepth*2) }
    println("Scoring time: $scoring_time")

    val titles = hashMapOf<Int, String>()
    val kickers = hashMapOf<Int, String>()
    for (sd in results.scoreDocs) {
        index.getField(sd.doc, "title")?.stringValue()?.let {
            titles.put(sd.doc, it)
        }
        index.getField(sd.doc, "kicker")?.stringValue()?.let {
            kickers.put(sd.doc, it)
        }
    }
    val titleDedup = results.scoreDocs
            // reject duplicates:
            .filter {
                val title = titles.get(it.doc)
                // hard-feature: has-title and most recent version:
                title != null && title != "null" && title != titles.get(it.doc+1)
            }
            .filterNot {
                // Reject opinion/editorial.
                skipTheseKickers.contains(kickers[it.doc] ?: "null")
            }

    val position = titleDedup.indexOfFirst { it.doc == docNo }
    val selfMRR = if (position < 0) {
        0.0
    } else {
        1.0 / (position+1)
    }
    val recommendations = titleDedup.filter { it.doc != docNo }
    println("Self-MRR: $selfMRR, ${recommendations.size}")

    return QueryResults(q.qid, recommendations.take(100).mapIndexed { idx, sd ->
        SimpleEvalDoc(index.getDocumentName(sd.doc), idx+1, sd.score.toDouble())
    })
}

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val NumTerms = argp.get("numFBTerms", 50)
    val numBigrams = argp.get("numBigrams", 20)
    val model = argp.get("model", "rdm")
    val includeFuture = argp.get("includeFuture", "yes")
    val includeFutureBool = includeFuture == "yes"

    val indexPath = File(argp.get("index", defaultWapoIndexPath))
    if (!indexPath.exists()) error("--index=$indexPath does not exist yet.")
    val queries = loadBGLinkQueries(argp.get("queries", "${System.getenv("HOME")}/code/queries/trec_news/newsir18-background-linking-topics.v2.xml"))


    File("umass_${model}.news.future_$includeFuture.bg.trecrun.gz").smartPrint { output ->
        IreneIndex(IndexParams().apply { withPath(indexPath) }).use { index ->
            index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)
            index.env.optimizeDirLog = false
            index.env.optimizeBM25 = true

            for (q in queries) {
                val docNo = index.documentById(q.docid) ?: error("Missing document for ${q.qid}")
                val lDoc = index.document(docNo) ?: error("Missing document fields for ${q.qid} internally: $docNo")
                val doc = fromLucene(index, lDoc)
                val tokens = index.tokenize(doc.body)

                when(model) {
                  "cbrdm" -> synchronized(index.env) {
                      index.env.estimateStats = "min"
                      val scorer: (QExpr)->QExpr = {q -> BM25Expr(q)}

                      val rmExpr = documentVectorToQuery(tokens, NumTerms, targetField = index.defaultField, scorer = scorer, numBigrams=numBigrams)
                      val qres = executeBackgroundLinkingQuery(q, index, docNo, doc, rmExpr, includeFuture=includeFutureBool)
                      qres.outputTrecrun(output, "umass_cbrdm")
                  }

                  "rm" -> synchronized(index.env) {
                      index.env.estimateStats = null
                      val rmExpr = documentVectorToQuery(tokens, NumTerms, targetField = index.defaultField, unigramWeight = 1.0, unigrams=true)
                      val qres = executeBackgroundLinkingQuery(q, index, docNo, doc, rmExpr, includeFuture=includeFutureBool)
                      qres.outputTrecrun(output, "umass_rm")
                  }

                  "rdm" -> synchronized(index.env) {
                      index.env.estimateStats = null
                      val rmExpr = documentVectorToQuery(tokens, NumTerms, targetField = index.defaultField, numBigrams=numBigrams)
                      val qres = executeBackgroundLinkingQuery(q, index, docNo, doc, rmExpr, includeFuture=includeFutureBool)
                      qres.outputTrecrun(output, "umass_rdm")
                  }

                  else -> TODO("Model=$model")
                }
            }
        }

    }

}
