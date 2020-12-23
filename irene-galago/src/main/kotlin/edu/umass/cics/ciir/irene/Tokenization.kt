package edu.umass.cics.ciir.irene

import org.lemurproject.galago.core.parse.TagTokenizer
import org.lemurproject.galago.utility.StringPooler
import kotlin.concurrent.getOrSet

class GalagoTokenizer : GenericTokenizer {
    init {
        StringPooler.disable()
    }
    private val local = ThreadLocal<TagTokenizer>()
    val tok: TagTokenizer get() = local.getOrSet { TagTokenizer() }
    override fun tokenize(input: String, field: String): List<String> {
        val gdoc = tok.tokenize(input)
        return gdoc.terms
    }
}

