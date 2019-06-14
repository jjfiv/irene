package edu.umass.cics.ciir.irene.trec2018.news

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.trec2018.TrecNewsWikiCount
import edu.umass.cics.ciir.irene.trec2018.TrecNewsWikiSource
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.smartPrint
import edu.unh.cs.treccar_v2.Data
import edu.unh.cs.treccar_v2.read_data.DeserializeData
import org.lemurproject.galago.utility.StreamCreator
import java.io.File

data class SimpleWikiPage(val id: String, val paragraphs: List<String>, val names: List<String>) {

}

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
                val altNames = ArrayList<String>()
                altNames.addAll(page.pageMetadata.disambiguationNames)
                altNames.addAll(page.pageMetadata.inlinkAnchors)
                altNames.addAll(page.pageMetadata.redirectNames)
                output.println(mapper.writeValueAsString(SimpleWikiPage(
                        page.pageId,
                        page.flatSectionPathsParagraphs().map { it.paragraph.textOnly },
                        altNames)))
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

data class WikiDictEntry(val id: String, val names: List<String>, val inlinks: Int) {

}

object BuildEntityDictionary {
    @JvmStatic fun main(args: Array<String>) {
        val mapper = ObjectMapper().registerKotlinModule()
        val msg = CountingDebouncer(TrecNewsWikiCount)
        File("/mnt/scratch/jfoley/wikidict.jsonl.gz").smartPrint { output ->
            DeserializeData.iterAnnotations(StreamCreator.openInputStream(TrecNewsWikiSource)).forEach { page ->
                if (page.pageType != Data.PageType.Article) {
                    return@forEach
                }
                val altNames = ArrayList<String>()
                altNames.addAll(page.pageMetadata.disambiguationNames)
                altNames.addAll(page.pageMetadata.inlinkAnchors)
                altNames.addAll(page.pageMetadata.redirectNames)
                val inlinks = page.pageMetadata.inlinkAnchors.size
                output.println(mapper.writeValueAsString(WikiDictEntry(
                        page.pageId,
                        altNames, inlinks)))
                msg.incr()?.let {
                    println("wiki-dictionary: ${it}")
                }
            }
        }
        println("Done!")
    }
}