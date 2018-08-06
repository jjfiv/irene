package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.LDoc
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import edu.umass.cics.ciir.irene.utils.forAllPairs
import edu.umass.cics.ciir.irene.utils.normalize
import edu.umass.cics.ciir.irene.utils.timed
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.jsoup.Jsoup
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.lang.Math.abs

data class TrecNewsBGLinkQuery(val qid: String, val docid: String, val url: String)

fun loadBGLinkQueries(path: String): List<TrecNewsBGLinkQuery> {
    val trecXMLDoc = File(path)
    if (!trecXMLDoc.exists()) error("TREC News BG Linking Query file not found at: $path")
    val doc = Jsoup.parse(trecXMLDoc, "UTF-8");
    return doc.select("top").map { query ->
        val qid = query.selectFirst("num").text().substringAfter("Number:").trim()
        val docid = query.selectFirst("docid").text().trim()
        val url = query.selectFirst("url").text().trim()
        TrecNewsBGLinkQuery(qid, docid, url)
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

val BackgroundLinkingDepth = 100;

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args);
    val NumTerms = argp.get("numFBTerms", 20)
    val indexPath = File(argp.get("index", "wapo.irene"))
    if (!indexPath.exists()) error("--index=$indexPath does not exist yet.")
    val queries = loadBGLinkQueries(argp.get("queries", "/Users/jfoley/code/queries/trec_news/newsir18-background-linking-topics.v2.xml"));
    val stopwords = inqueryStop

    IreneIndex(IndexParams().apply { withPath(indexPath) }).use { index ->
        for (q in queries) {
            val docNo = index.documentById(q.docid) ?: error("Missing document for ${q.qid}")
            val lDoc = index.document(docNo) ?: error("Missing document fields for ${q.qid} internally: $docNo")
            val doc = fromLucene(index, lDoc);
            //val docP = index.docAsParameters(docNo) ?: error("Missing document fields for ${q.qid}")
            val tokens = index.tokenize(doc.body)

            val time = doc.published_date ?: 0L;
            assert(time >= 0L);

            val publishedBeforeExpr = LongLTE(DenseLongField("published_date"), time)
            val countBefore = index.count(publishedBeforeExpr)

            val counts = TObjectIntHashMap<String>()
            val length = tokens.size.toDouble()
            for (t in tokens) {
                if (stopwords.contains(t)) continue
                counts.adjustOrPutValue(t, 1, 1);
            }
            val weights = TObjectDoubleHashMap<String>()
            counts.forEachEntry {term, count ->
                weights.put(term, count / length)
                true
            }
            val rm = RelevanceModel(weights, index.defaultField)
            val terms = rm.toTerms(NumTerms).map { it.term }

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
                if (min <= 8) {
                    mins.put(Pair(lhs, rhs), min)
                }
            }

            val bigramWeights = arrayListOf<Double>()
            val bigramNodes = mins.keys.map { (lhs, rhs) ->
                val importance = rm.weights[lhs] * rm.weights[rhs]
                bigramWeights.add(importance)
                DirQLExpr(SmallerCountExpr(listOf(TextExpr(lhs), TextExpr(rhs)))).weighted(importance)
            };
            val uww = CombineExpr(bigramNodes, bigramWeights.normalize())

            val rmExpr = SumExpr(rm.toQExpr(NumTerms).weighted(0.8), uww.weighted(0.2));
            val finalExpr = RequireExpr(AndExpr(listOf(rmExpr, publishedBeforeExpr)), rmExpr).deepCopy()

            println("${q.qid}, $docNo ${doc.title}\n\t${doc.url}\n\t${terms}\n\t${countBefore}")

            val (scoring_time, results) = timed { index.search(finalExpr, BackgroundLinkingDepth*2) }
            println("Scoring time: $scoring_time")

            val titles = hashMapOf<Int, String>()
            for (sd in results.scoreDocs) {
                index.getField(sd.doc, "title")?.stringValue()?.let {
                    titles.put(sd.doc, it)
                }
            }
            val titleDedup = results.scoreDocs
                    // reject duplicates:
                    .filter {
                        val title = titles.get(it.doc)
                        // hard-feature: has-title and most recent version:
                        title != null && title != "null" && title != titles.get(it.doc+1)
                    }
                    // TODO: reject opinion/editorial?

            val position = titleDedup.indexOfFirst { it.doc == docNo }
            val selfMRR = if (position < 0) {
                0.0
            } else {
                1.0 / (position+1)
            }
            val recommendations = titleDedup.filter { it.doc != docNo }
            println("Self-MRR: $selfMRR, ${recommendations.size}")
            for (r in recommendations.take(5)) {
                val title = index.getField(r.doc, "title")?.stringValue()
                val id = index.getField(r.doc, "id")!!.stringValue()
                println("\t\t${r.doc} ... ${id} ... ${title}");
            }
        }
    }
}