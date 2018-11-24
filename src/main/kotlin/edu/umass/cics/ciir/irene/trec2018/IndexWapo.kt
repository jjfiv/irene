package edu.umass.cics.ciir.irene.trec2018

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.utils.smartDoLines
import org.jsoup.Jsoup
import org.lemurproject.galago.utility.Parameters
import java.io.File

@JsonIgnoreProperties(ignoreUnknown = true)
data class WapoContent(
        val content: Any?,
        val mime: String?,
        val type: String?,
        val subtype: String?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class WapoArticle(
        val id: String,
        val article_url: String?,
        val type: String?,
        val source: String?,
        val title: String?,
        val author: String?,
        val published_date: Long?,
        val contents: List<WapoContent?>?
)

val defaultWapoIndexPath = "/mnt/scratch/jfoley/trec-news-2018/wapo.irene"

fun main(args: Array<String>) {
    val mapper = ObjectMapper().registerKotlinModule()
    val argp = Parameters.parseArgs(args)

    val input = File(argp.get("input", "/mnt/scratch/jfoley/trec-news-2018/WashingtonPost.v2/data/TREC_Washington_Post_collection.v2.jl.gz"))
    assert(input.exists())
    val params = IndexParams().apply {
        create()
        withPath(File(argp.get("index", defaultWapoIndexPath)))
    }

    val index = IreneIndexer(params).use { writer ->
        input.smartDoLines(doProgress=true, total=595037) { line ->
            val article = mapper.readValue(line, WapoArticle::class.java)
            writer.doc {
                setId(article.id)
                setDenseLongField("published_date", article.published_date ?: 0L)
                setStringField("url", article.article_url ?: "MISSING")
                article.type?.let { kind ->
                    setStringField("kind", kind)
                }
                maybeTextField("title", article.title)
                maybeTextField("author", article.author)

                // Collect disjoint paragraphs:
                val paragraphs = ArrayList<String>()
                val blobs = (article.contents ?: emptyList()).filterNotNull();

                // So we can elide Opinion and Editorial content
                val kicker = blobs.find { it.type == "kicker" }?.content
                if (kicker != null && kicker is String) {
                    setTextField("kicker", kicker)
                }

                for (blob in blobs.filter { it.subtype == "paragraph" }) {
                    if (blob.content is String) {
                        val html = Jsoup.parse(blob.content).text()
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