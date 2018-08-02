package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.unh.cs.treccar_v2.read_data.DeserializeData
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File


fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val input = File(argp.get("input", "/mnt/scratch/jfoley/trec-news-2018/all-enwiki-20170820/all-enwiki-20170820.cbor.gz"))
    assert(input.exists())

    val params = IndexParams().apply {
        create()
        withPath(File(argp.get("index", "/mnt/scratch/jfoley/trec-news-2018/news.irene")))
    }
    val msg = CountingDebouncer(7_100_813L)

    val index = IreneIndexer(params).use { writer ->
        DeserializeData.iterAnnotations(StreamCreator.openInputStream(input)).forEach { page ->
            writer.doc {
                setId(page.pageId)
                setTextField("title", page.pageName)
                val altNames = ArrayList<String>()
                altNames.addAll(page.pageMetadata.disambiguationNames)
                altNames.addAll(page.pageMetadata.inlinkAnchors)
                setTextField("names", altNames.joinToString(separator="\t"))
                setTextField(params.defaultField, page.flatSectionPathsParagraphs().joinToString(separator = "\n\n") { it.paragraph.textOnly })
            }
            msg.incr()?.let {
                println("Indexing TREC News Wikipedia: ${it}")
            }
        }
        writer.open()
    }

    index.use {
        println("Indexed ${it.reader.numDocs()} pages for TREC News.");
    }
}
