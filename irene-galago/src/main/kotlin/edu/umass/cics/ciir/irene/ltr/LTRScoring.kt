package edu.umass.cics.ciir.irene.ltr

import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.index.IIndex
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.RREnv
import edu.umass.cics.ciir.irene.scoring.*
import edu.umass.cics.ciir.irene.utils.IntList
import org.apache.lucene.index.Term
import org.apache.lucene.search.DocIdSetIterator
import org.apache.lucene.search.Explanation
import org.lemurproject.galago.utility.Parameters

/**
 *
 * @author jfoley.
 */


interface ILTRDocField {
    val name: String
    val text: String
    val tokenizer: GenericTokenizer
    val terms: List<String>
    val freqs: LanguageModel<String>
    val length: Int
    val uniqTerms: Int
    val termSet: Set<String>
    fun toEntry(): Pair<String, ILTRDocField> = Pair(name, this)
    fun count(term: String): Int
}

data class LTRBagDocField(override val name: String, override val freqs: LanguageModel<String>): ILTRDocField {
    override val text: String get() = throw UnsupportedOperationException()
    override val tokenizer get() = throw UnsupportedOperationException()
    override fun count(term: String): Int = freqs.count(term)
    override val terms: List<String> get() = throw UnsupportedOperationException()
    override val length: Int = freqs.length.toInt()
    override val uniqTerms: Int get() = termSet.size
    override val termSet: Set<String> get() = freqs.termSet()
}

data class LTREmptyDocField(override val name: String) : ILTRDocField {
    override val text: String = ""
    override val tokenizer: GenericTokenizer = WhitespaceTokenizer()
    override val terms: List<String> = emptyList()
    override val freqs: LanguageModel<String> = BagOfWords(emptyList())
    override val length: Int = 1
    override val uniqTerms: Int = 0
    override val termSet: Set<String> = emptySet()
    override fun count(term: String): Int = 0
}

data class LTRMergedField(override val name: String, val components: List<ILTRDocField>) : ILTRDocField {
    override val tokenizer: GenericTokenizer get() = components[0].tokenizer
    override val text: String by lazy { components.joinToString(separator = "\n") { it.text } }
    override val terms: List<String> by lazy { components.flatMap { it.terms } }
    override val freqs: LanguageModel<String> by lazy { BagOfWords(terms) }
    override val length: Int get() = terms.size
    override val uniqTerms: Int get() = freqs.termSet().size
    override fun count(term: String): Int = freqs.count(term)
    override val termSet: Set<String> get() = freqs.termSet()
}

data class LTRDocField(override val name: String, override val text: String, override val tokenizer: GenericTokenizer = WhitespaceTokenizer()) : ILTRDocField {
    override val terms: List<String> by lazy { tokenizer.tokenize(text, name) }
    override val freqs: BagOfWords by lazy { BagOfWords(terms) }
    override val length: Int get() = terms.size
    override val uniqTerms: Int get() = freqs.counts.size()
    override fun count(term: String): Int = freqs.count(term)
    override val termSet: Set<String> get() = freqs.counts.keySet()
}

data class LTRTokenizedDocField(override val name: String, override val terms: List<String>, override val tokenizer: GenericTokenizer) : ILTRDocField {
    override val text: String by lazy { terms.joinToString(separator = " ")}
    override val freqs: BagOfWords by lazy { BagOfWords(terms) }
    override val length: Int get() = terms.size
    override val uniqTerms: Int get() = freqs.counts.size()
    override fun count(term: String): Int = freqs.count(term)
    override val termSet: Set<String> get() = freqs.counts.keySet()
}

interface ILTRDoc {
    val name: String
    fun field(field: String): ILTRDocField
    fun terms(field: String): List<String>
    fun freqs(field: String): LanguageModel<String>
    fun hasField(field: String): Boolean
}

fun ScoringEnv.ltr() = (this as LTRDocScoringEnv).ltr

/**
 * Lazy implementation of [ILTRDoc] based upon [ILTRDocField].
 */
data class LTRDoc(override val name: String, val fields: Map<String, ILTRDocField>) : ILTRDoc {
    override fun field(field: String): ILTRDocField = fields[field] ?: error("No such field: $field in $this.")
    override fun terms(field: String) = field(field).terms
    override fun freqs(field: String) = field(field).freqs
    override fun hasField(field: String) = fields.contains(field)

    constructor(name: String, field: ILTRDocField) : this(name, mapOf(field.toEntry()))
    constructor(name: String, text: String, field: String, tokenizer: GenericTokenizer = WhitespaceTokenizer())
            : this(name, hashMapOf(LTRDocField(field, text, tokenizer).toEntry()))

    fun eval(env: RREnv, q: QExpr): Double {
        val expr = RREvalNodeExpr(env, q)
        return expr.eval(this)
    }

    companion object {
        fun create(id: String, fjson: Parameters, fields: Set<String>, index: IIndex): LTRDoc = create(id, fjson, fields, index.tokenizer)
        fun create(id: String, fjson: Parameters, fields: Set<String>, tokenizer: GenericTokenizer): LTRDoc {
            val ltrFields = HashMap<String, ILTRDocField>()

            fjson.keys.forEach { key ->
                if (fjson.isString(key)) {
                    val fieldText = fjson.getStr(key)
                    ltrFields.put(key, LTRDocField(key, fieldText, tokenizer))
                } else {
                    println("Warning: Can't handle field: $key=${fjson[key]}")
                }
            }

            // Create empty fields as needed.
            fields
                    .filterNot { ltrFields.containsKey(it) }
                    .forEach { ltrFields[it] = LTREmptyDocField(it) }

            return LTRDoc(id, ltrFields)
        }
    }
}

data class LTRDocScoringEnv(val ltr: ILTRDoc) : ScoringEnv(ltr.name.hashCode()) {
    override fun toString(): String {
        return "LTRDocScoringEnv(${ltr.name}, $doc)"
    }
}

class LTREvalSetupContext(override val env: RREnv) : EvalSetupContext {
    override fun luceneIter(query: LuceneQuery): DocIdSetIterator = TODO("lucene-ltr")
    override fun setupLuceneRaw(q: LuceneQuery): QueryEvalNode? = TODO("lucene-ltr")
    override fun denseLongField(name: String, missing: Long): QueryEvalNode = TODO("lucene-ltr")
    val termCache = HashMap<Term, LTRDocTerm>()
    val lengthsCache = HashMap<String, LTRDocLength>()
    override fun create(term: Term, needed: DataNeeded): QueryEvalNode = termCache.computeIfAbsent(term, {
        LTRDocTerm(it.field(), it.text())
    })
    override fun createLengths(field: String): QueryEvalNode = lengthsCache.computeIfAbsent(field, { LTRDocLength(it) })
    override fun numDocs(): Int = 42

    /** TODO refactor this to be a createDoclistMatcher for [WhitelistExpr] as [WhitelistMatchEvalNode]. */
    override fun selectRelativeDocIds(ids: List<Int>): IntList = IntList(-1)
}


abstract class LTRDocFeatureNode : QueryEvalNode {
    override val children: List<QueryEvalNode> = emptyList()
    override fun explain(env: ScoringEnv): Explanation = if (matches(env)) {
        Explanation.match(score(env).toFloat(), "${this.javaClass.simpleName} count=${count(env)} score=${score(env)} matches=${matches(env)}")
    } else {
        Explanation.noMatch("${this.javaClass.simpleName} count=${count(env)} score=${score(env)} matches=${matches(env)}")
    }
    override fun estimateDF(): Long = 0
}

data class LTRDocLength(val field: String) : CountEvalNode, LTRDocFeatureNode() {
    override fun count(env: ScoringEnv): Int = env.ltr().field(field).length
    override fun matches(env: ScoringEnv): Boolean = env.ltr().hasField(field)
}

data class LTRDocTerm(val field: String, val term: String): PositionsEvalNode, LTRDocFeatureNode() {
    var cache: Pair<String, IntArray>? = null
    override fun positions(env: ScoringEnv): PositionsIter {
        val doc = env.ltr()
        val c = cache
        if (c != null && doc.name == c.first) {
            return PositionsIter(c.second)
        }

        val vec = doc.field(field).terms
        val hits = vec.mapIndexedNotNull { i, t_i ->
            if (t_i == term) i else null
        }.toIntArray()
        cache = Pair(doc.name, hits)
        return PositionsIter(hits)
    }
    override fun count(env: ScoringEnv): Int = env.ltr().field(field).count(term)
    override fun matches(env: ScoringEnv): Boolean = count(env) > 0
    override fun explain(env: ScoringEnv): Explanation = if (matches(env)) {
        Explanation.match(score(env).toFloat(), "${this.javaClass.simpleName} count=${count(env)} score=${score(env)} matches=${matches(env)} positions=${positions(env)}")
    } else {
        Explanation.noMatch("${this.javaClass.simpleName} count=${count(env)} score=${score(env)} matches=${matches(env)} positions=${positions(env)}")
    }
}

