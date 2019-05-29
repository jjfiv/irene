package edu.umass.cics.ciir.irene.trec2018.news

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.trec2018.TrecNewsWikiCount
import edu.umass.cics.ciir.irene.trec2018.TrecNewsWikiSource
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.smartPrint
import edu.unh.cs.treccar_v2.read_data.DeserializeData
import org.lemurproject.galago.utility.StreamCreator
import java.io.File

fun main() {
    val queries = loadEntityLinkQueries("${System.getenv("HOME")}/code/queries/trec_news/newsir18-entity-ranking-topics.xml")
    val pageNames = queries.flatMapTo(HashSet()) { q ->
        q.ent.map { e ->
            e.link
        }
    }
    val mapper = ObjectMapper().registerKotlinModule()
    val msg = CountingDebouncer(TrecNewsWikiCount)
    var found = 0
    File("news2018.wikipage.jsonl").smartPrint { output ->
        DeserializeData.iterAnnotations(StreamCreator.openInputStream(TrecNewsWikiSource)).forEach { page ->
            if (pageNames.contains(page.pageId)) {
                output.println(mapper.writeValueAsString(page))
                found += 1
                if (found == pageNames.size) {
                    return@smartPrint
                }
            }
            msg.incr()?.let {
                println("Finding TREC News Wikipedia: ${found}/${pageNames.size} ${it}")
            }
        }
    }
    println("Done!")
}
