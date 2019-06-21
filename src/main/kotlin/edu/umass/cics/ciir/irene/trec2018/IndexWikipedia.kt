package edu.umass.cics.ciir.irene.trec2018

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.smartLines
import edu.unh.cs.treccar_v2.Data
import edu.unh.cs.treccar_v2.read_data.DeserializeData
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File
import java.util.concurrent.ConcurrentHashMap

val TrecNewsWikiSource = "/mnt/scratch/jfoley/trec-news-2018/all-enwiki-20170820/all-enwiki-20170820.cbor.gz"
val TrecNewsWikiCount = 7_100_813L
val TrecNewsWikiPageRank = "/mnt/scratch/jfoley/trec-news-2018/newsir.wiki.pagerank.gz"
val TrecNewsWikiIndex = "/mnt/scratch/jfoley/trec-news-2018/wiki.v2.irene"

fun loadWikiPageRank(path: String): Map<String, Float> {
    val byName = ConcurrentHashMap<String, Float>()
    val msg = CountingDebouncer(total=6806206)
    File(path).smartLines { lines ->
        for (line in lines) {
            msg.incr()?.let { upd -> println("PageRank load: $upd")}
            val row = line.trim().split('\t')
            val id = row[0]
            val score = row[1].toFloat()
            byName.put(id, score)
        }
    }
    return byName
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val input = File(argp.get("input", TrecNewsWikiSource))
    assert(input.exists())
    val pageRanks = loadWikiPageRank(TrecNewsWikiPageRank)
    val minPageRank = pageRanks.values.min()!!

    val params = IndexParams().apply {
        create()
        withPath(File(argp.get("index", TrecNewsWikiIndex)))
    }
    val msg = CountingDebouncer(TrecNewsWikiCount)

    val index = IreneIndexer(params).use { writer ->
        DeserializeData.iterAnnotations(StreamCreator.openInputStream(input)).forEach { page ->
            msg.incr()?.let {
                println("Indexing TREC News Wikipedia: ${it}")
            }
            when(page.pageType) {
                Data.PageType.Article -> {}
                null,
                Data.PageType.Category,
                Data.PageType.Disambiguation,
                Data.PageType.Redirect -> return@forEach
            }
            writer.doc {
                setId(page.pageId)
                setTextField("title", page.pageName)
                val altNames = ArrayList<String>()
                altNames.addAll(page.pageMetadata.disambiguationNames)
                altNames.addAll(page.pageMetadata.inlinkAnchors)
                altNames.addAll(page.pageMetadata.redirectNames)

                setBoolField("listOf", page.pageName.startsWith("List"), stored=false)
                setDenseFloatField("pageRank", pageRanks.get(page.pageId) ?: minPageRank, true)
                setTextField("names", altNames.joinToString(separator="\t"))
                setTextField("categories", page.pageMetadata.categoryNames.joinToString(separator="\t"))
                setTextField(params.defaultField, page.flatSectionPathsParagraphs().joinToString(separator = "\n\n") { it.paragraph.textOnly })
            }

        }
        writer.commit()
        if (argp.get("compactIndex", true)) {
            writer.writer.forceMerge(4)
        }
        writer.open()
    }

    index.use {
        println("Indexed ${it.reader.numDocs()} pages for TREC News.");
    }
}
