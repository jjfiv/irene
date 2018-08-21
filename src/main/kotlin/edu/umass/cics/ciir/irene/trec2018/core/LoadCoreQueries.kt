package edu.umass.cics.ciir.irene.trec2018.core

import org.jsoup.Jsoup
import org.lemurproject.galago.utility.Parameters
import java.io.File

data class TrecCoreQuery(val qid: String, val title: String, val desc: String, val narr: String) {
}

fun LoadTrecCoreQueries(path: String): Map<String, TrecCoreQuery> {
    val trecXMLDoc = File(path)
    if (!trecXMLDoc.exists()) error("TREC Core Query file not found at: $path")
    val doc = Jsoup.parse(trecXMLDoc, "UTF-8")
    return doc.select("top").map { query ->
        val qid = query.selectFirst("num").text().substringAfter("Number:").trim()
        val title = query.selectFirst("title").text().trim()
        val desc = query.selectFirst("desc").text().substringAfter("Description:").trim()
        val narr = query.selectFirst("narr").text().substringAfter("Narrative").trim()
        TrecCoreQuery(qid, title, desc, narr)
    }.associateBy { it.qid }
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val queryFile = argp.get("queryFile", listOf(System.getenv("HOME"), "code", "queries", "trec_core", "2018-test-topics.txt").joinToString(separator=File.separator))
    val queries = LoadTrecCoreQueries(queryFile)
    println(queries.entries.first())
}