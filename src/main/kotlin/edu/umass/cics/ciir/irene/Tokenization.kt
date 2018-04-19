package edu.umass.cics.ciir.irene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.LowerCaseFilter
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.en.KStemFilter
import org.apache.lucene.analysis.standard.StandardFilter
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.lemurproject.galago.core.parse.TagTokenizer
import org.lemurproject.galago.utility.StringPooler
import kotlin.concurrent.getOrSet
import kotlin.coroutines.experimental.buildSequence

/**
 *
 * @author jfoley.
 */
class IreneEnglishAnalyzer : Analyzer() {
    override fun createComponents(fieldName: String?): TokenStreamComponents {
        val source = StandardTokenizer()
        var result: TokenStream = StandardFilter(source)
        result = LowerCaseFilter(result)
        result = KStemFilter(result)
        return TokenStreamComponents(source, result)
    }
}

fun Analyzer.tokenize(field: String, input: String): List<String> = this.tokenSequence(field, input).toList()

fun Analyzer.tokenSequence(field: String, input: String): Sequence<String> = buildSequence {
    tokenStream(field, input).use { body ->
        val charTermAttr = body.addAttribute(CharTermAttribute::class.java)

        // iterate over tokenized field:
        body.reset()
        while(body.incrementToken()) {
            yield(charTermAttr.toString())
        }
    }
}

interface GenericTokenizer {
    fun tokenize(fields: Map<String, String>) = fields.mapValues { (field, text) -> tokenize(text, field) }
    fun tokenize(input: String, field: String): List<String>
}
class WhitespaceTokenizer() : GenericTokenizer {
    val ws = "\\s+".toRegex()
    override fun tokenize(input: String, field: String): List<String> = input.split(ws)
}
class LuceneTokenizer(val analyzer: Analyzer) : GenericTokenizer {
    override fun tokenize(input: String, field: String): List<String> = analyzer.tokenize(field, input)
}
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

