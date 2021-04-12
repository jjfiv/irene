package edu.umass.cics.ciir.irene.apps

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.indexing.IndexParams
import java.io.File

fun main(args: Array<String>) {
    val mapper: ObjectMapper = ObjectMapper()
        .registerKotlinModule()

    IndexParams().apply {
        withPath(File(args[0]))
    }.openReader().use { reader ->
        for (i in 0..reader.reader.numDocs()) {
            val fields = reader.docAsMap(i)
            println(mapper.writeValueAsString(fields))
        }
    }
}