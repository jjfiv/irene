package edu.umass.cics.ciir.irene.trec2018.car

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.unh.cs.treccar_v2.read_data.DeserializeData
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File


fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val input = File(argp.get("input", "/mnt/scratch/jfoley/trec-car-v2/paragraphCorpus/dedup.articles-paragraphs.cbor"))
    assert(input.exists())

    val params = IndexParams().apply {
        create()
        withPath(File(argp.get("index", "/mnt/scratch/jfoley/trec-car-v2/car.irene")))
    }
    val msg = CountingDebouncer(29794697L)

    val index = IreneIndexer(params).use { writer ->
        DeserializeData.iterParagraphs(StreamCreator.openInputStream(File(""))).forEach { para ->
            writer.doc {
                setId(para.paraId)
                setTextField(params.defaultField, para.textOnly, false)
                setTextField("ent", para.entitiesOnly.joinToString(separator = "\t"))
            }
            msg.incr()?.let {
                println("Indexing TREC CAR: ${it}")
            }
        }
        writer.open()
    }

    index.use {
        println("Indexed ${it.reader.numDocs()} paragraphs for TREC-CAR.");
    }
}