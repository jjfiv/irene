package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.jsoup.Jsoup
import org.lemurproject.galago.utility.Parameters
import java.io.File

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

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args);
    val indexPath = File(argp.get("index", "/mnt/scratch/jfoley/trec-news-2018/wapo.irene"))
    if (!indexPath.exists()) error("--index=$indexPath does not exist yet.")
    val queries = loadBGLinkQueries(argp.get("queries", "/home/jfoley/code/queries/trec_news/newsir18-background-linking-topics.v2.xml"));
    val stopwords = inqueryStop

    IreneIndex(IndexParams().apply { withPath(indexPath) }).use { index ->
        for (q in queries) {
            val docNo = index.documentById(q.docid) ?: error("Missing document for ${q.qid}")
            val docP = index.docAsParameters(docNo) ?: error("Missing document fields for ${q.qid}")
            val tokens = index.tokenize(docP.getString(index.defaultField) ?: "")

            val time = docP.get("published_date", 0L)

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
            val terms = rm.toTerms(10).map { it.term }

            println("${q.qid}, $docNo ${docP.get("title")}\n\t${terms}")
        }
    }
}