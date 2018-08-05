package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.LDoc
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.AndExpr
import edu.umass.cics.ciir.irene.lang.DenseLongField
import edu.umass.cics.ciir.irene.lang.LongLTE
import edu.umass.cics.ciir.irene.lang.RequireExpr
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import edu.umass.cics.ciir.irene.utils.timed
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

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args);
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
            val terms = rm.toTerms(10).map { it.term }
            val rmExpr = rm.toQExpr(100)
            val finalExpr = RequireExpr(AndExpr(listOf(rmExpr, publishedBeforeExpr)), rmExpr).deepCopy()

            println("${q.qid}, $docNo ${doc.title}\n\t${terms}\n\t${countBefore}")

            val (scoring_time, results) = timed { index.search(finalExpr, 100) }

            val before_time = results.scoreDocs.map {
                (index.getField(it.doc, "published_date")?.numericValue() ?: 0L).toLong()
            }.count { pd -> pd <= time }
            println("Found appropriate timeliness: ${before_time}/${results.scoreDocs.size}")
            if (results.scoreDocs.mapTo(HashSet()) { it.doc } .contains(docNo)) {
                println("Found self in ${scoring_time}")
            }
        }
    }
}