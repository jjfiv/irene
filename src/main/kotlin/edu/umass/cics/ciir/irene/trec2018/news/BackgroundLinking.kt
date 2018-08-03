package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import org.jsoup.Jsoup
import org.lemurproject.galago.utility.Parameters
import java.io.File

data class TrecNewsBGLinkQuery(val qid: String, val docid: String, val url: String)

const val NoHyphenId =    "b4c6361974466458bb721b9b1628220b";
const val NoHyphenIdFix = "b4c63619-7446-6458-bb72-1b9b1628220b";
const val HyphenId =      "e1336b8f-b0c2-4610-9a3c-ec85a546c9ad"
fun hyphenateId(input: String): String {
    if (input.length < HyphenId.length) {
        return "${input.substring(0,8)}-${input.substring(8,12)}-${input.substring(12,16)}-${input.substring(16,20)}-${input.substring(20)}"
    }
    return input
}

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
    assert(hyphenateId(NoHyphenId) == NoHyphenIdFix)
    assert(hyphenateId(HyphenId) == HyphenId)
    val argp = Parameters.parseArgs(args);
    val indexPath = File(argp.get("index", "/mnt/scratch/jfoley/trec-news-2018/wapo.irene"))
    if (!indexPath.exists()) error("--index=$indexPath does not exist yet.")
    val queries = loadBGLinkQueries(argp.get("queries", "/home/jfoley/code/queries/trec_news/newsir18-background-linking-topics.v2.xml"));

    IreneIndex(IndexParams().apply { withPath(indexPath) }).use { index ->
        for (q in queries) {
            val docNo = index.documentById(q.docid)
            if (docNo == null) {
                println("$q, $docNo")
            }
        }
    }
}