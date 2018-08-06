package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.smartPrint
import edu.unh.cs.treccar_v2.read_data.DeserializeData
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val input = File(argp.get("input", "/mnt/scratch/jfoley/trec-news-2018/all-enwiki-20170820/all-enwiki-20170820.cbor.gz"))
    assert(input.exists())

    val msg = CountingDebouncer(7_100_813L)
    File("/mnt/scratch/jfoley/trec-news-2018/wiki.src_tgt.tsv.gz").smartPrint { edges ->
        File("/mnt/scratch/jfoley/trec-news-2018/wiki.cat.tsv.gz").smartPrint { cats ->
            DeserializeData.iterAnnotations(StreamCreator.openInputStream(input)).forEach { page ->
                for (source in page.pageMetadata.inlinkIds) {
                    edges.println("$source\t${page.pageId}")
                }
                for (cat in page.pageMetadata.categoryIds) {
                    cats.println("${page.pageId}\t${cat}")
                }

                msg.incr()?.let { update ->
                    println("Creating link and category graph $update")
                }
            }
        }
    }
    println("Succeeded in creating link and category graph.")
}