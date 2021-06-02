package edu.umass.cics.ciir.irene.apps

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.indexing.IndexParams
import org.apache.lucene.index.MultiFields
import org.apache.lucene.index.MultiTerms
import org.apache.lucene.index.TermContext
import java.io.File

fun main(args: Array<String>) {
    val mapper: ObjectMapper = ObjectMapper()
        .registerKotlinModule()

    val defaultField = if (args.size == 1) "body" else  args[1]

    IndexParams().apply {
        withPath(File(args[0]))
    }.openReader().use { reader ->
        val terms = MultiFields.getTerms(reader.reader, defaultField)
        val lastTerm = terms.max
        val firstTerm = terms.min
        val t_enum = terms.iterator()
        t_enum.seekExact(firstTerm)
        var i = 0L
        while (true) {
            val df = t_enum.docFreq()
            val tf = t_enum.totalTermFreq()
            val token = t_enum.term().utf8ToString()
            if (df >= 2) {
                println("$token\t$df\t$tf")
            }

            if (t_enum.term() == lastTerm) {
                break
            }
            i += 1
            t_enum.next()
        }

    }
}
