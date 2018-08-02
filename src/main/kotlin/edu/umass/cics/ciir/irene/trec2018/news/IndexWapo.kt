package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.utils.smartDoLines
import org.jsoup.Jsoup
import org.lemurproject.galago.utility.Parameters
import java.io.File

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val input = File(argp.get("input", "/mnt/scratch/jfoley/trec-news-2018/WashingtonPost.v2/data/TREC_Washington_Post_collection.v2.jl.gz"))
    assert(input.exists())
    val params = IndexParams().apply {
        create()
        withPath(File(argp.get("index", "/mnt/scratch/jfoley/trec-news-2018/wapo.irene")))
    }

    val index = IreneIndexer(params).use { writer ->
        input.smartDoLines(doProgress=true, total=595037) { line ->
            val jdoc = Parameters.parseString(line)
            writer.doc {
                setId(jdoc.getStr("id"))
                setDenseLongField("published_date", jdoc.getLong("published_date"))
                setStringField("url", jdoc.get("article_url", "MISSING"))
                maybeTextField("title", jdoc.getString("title"))
                maybeTextField("author", jdoc.getString("author"))

                // Collect disjoint paragraphs:
                val paragraphs = ArrayList<String>()
                for (content in jdoc.getAsList("contents", Parameters::class.java)) {
                    if (content.getString("subtype") == "paragraph") {
                        val html = Jsoup.parse(content.get("content", "")).text()
                        paragraphs.add(html)
                    }
                }

                val text = paragraphs.joinToString(separator = "\n\n")
                setTextField(params.defaultField, text)
            }
        }
        writer.commit()
        writer.open()
    }

    index.use {
        println("Indexed ${it.reader.numDocs()} articles for TREC-News.");
    }
}