package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.DataNeeded
import edu.umass.cics.ciir.irene.GenericTokenizer
import edu.umass.cics.ciir.irene.IIndex
import edu.umass.cics.ciir.irene.WhitespaceTokenizer
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.galago.incr
import edu.umass.cics.ciir.irene.galago.pmake
import edu.umass.cics.ciir.irene.lang.DirQLExpr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.SumExpr
import edu.umass.cics.ciir.irene.lang.TextExpr
import edu.umass.cics.ciir.irene.ltr.RREnv
import edu.umass.cics.ciir.irene.ltr.RRExpr
import edu.umass.cics.ciir.irene.ltr.WeightedTerm
import edu.umass.cics.ciir.irene.ltr.normalized
import edu.umass.cics.ciir.irene.utils.IntList
import gnu.trove.map.hash.TObjectIntHashMap
import org.apache.lucene.index.Term
import org.apache.lucene.search.Explanation
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters

/**
 *
 * @author jfoley.
 */

class BagOfWords(terms: List<String>) {
    val counts = TObjectIntHashMap<String>()
    val length = terms.size.toDouble()
    val l2norm: Double by lazy {
        var sumSq = 0.0
        counts.forEachValue { c ->
            sumSq += c*c
            true
        }
        Math.sqrt(sumSq)
    }
    init {
        terms.forEach { counts.adjustOrPutValue(it, 1, 1) }
    }
    fun prob(term: String): Double = counts.get(term) / length
    fun count(term: String): Int {
        if (!counts.containsKey(term)) return 0
        return counts[term]
    }
    private fun toTerms(): List<WeightedTerm> {
        val output = ArrayList<WeightedTerm>(counts.size())
        counts.forEachEntry {term, weight ->
            output.add(WeightedTerm(weight.toDouble(), term))
        }
        return output
    }
    fun toTerms(k: Int): List<WeightedTerm> = toTerms().sorted().take(k).normalized()
    fun toQExpr(k: Int, scorer: (TextExpr)-> QExpr = { DirQLExpr(it) }, targetField: String? = null, statsField: String? = null) = SumExpr(toTerms(k).map {
        scorer(TextExpr(it.term, field = targetField, statsField = statsField)).weighted(it.score)
    })

}

interface ILTRDocField {
    val name: String
    val text: String
    val tokenizer: GenericTokenizer
    val terms: List<String>
    val freqs: BagOfWords
    val length: Int
    val uniqTerms: Int
    val termSet: Set<String>
    fun toEntry(): Pair<String, ILTRDocField> = Pair(name, this)
    fun count(term: String): Int
}

data class LTREmptyDocField(override val name: String) : ILTRDocField {
    override val text: String = ""
    override val tokenizer: GenericTokenizer = WhitespaceTokenizer()
    override val terms: List<String> = emptyList()
    override val freqs: BagOfWords = BagOfWords(emptyList())
    override val length: Int = 1
    override val uniqTerms: Int = 0
    override val termSet: Set<String> = emptySet()
    override fun count(term: String): Int = 0
}

data class LTRMergedField(override val name: String, val components: List<ILTRDocField>) : ILTRDocField {
    override val tokenizer: GenericTokenizer get() = components[0].tokenizer
    override val text: String by lazy { components.joinToString(separator = "\n") { it.text } }
    override val terms: List<String> by lazy { components.flatMap { it.terms } }
    override val freqs: BagOfWords by lazy { BagOfWords(terms) }
    override val length: Int get() = terms.size
    override val uniqTerms: Int get() = freqs.counts.size()
    override fun count(term: String): Int = freqs.count(term)
    override val termSet: Set<String> get() = freqs.counts.keySet()
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


data class LTRDoc(val name: String, val features: HashMap<String, Double>, val fields: HashMap<String,ILTRDocField>, var defaultField: String = "document") {
    fun field(field: String): ILTRDocField = fields[field] ?: error("No such field: $field in $this.")
    fun terms(field: String) = field(field).terms
    fun freqs(field: String) = field(field).freqs

    fun target() = field(defaultField)

    constructor(name: String, text: String, field: String, tokenizer: GenericTokenizer = WhitespaceTokenizer()): this(name, hashMapOf(), hashMapOf(LTRDocField(field, text, tokenizer).toEntry()), field)

    fun toJSONFeatures(qrels: QueryJudgments, qid: String) = pmake {
        set("label", qrels[name])
        set("qid", qid)
        set("features", Parameters.wrap(features))
        set("name", name)
    }

    fun toJSONDoc() = pmake {
        set("features", Parameters.wrap(features))
        set("id", name)
        set("fields", Parameters.wrap(fields.mapValues { (_, f) -> f.text }))
    }

    fun featureForRanking(name: String, fallback: Double = Double.NEGATIVE_INFINITY) = features[name] ?: fallback;


    /**
     * Take in a list of [feature_exprs] to evaluate and output the count of [errors] (NaN or Inf) per feature.
     */
    fun evalAndSetFeatures(feature_exprs: Map<String, RRExpr>, errors: MutableMap<String, Int>? = null) {
        feature_exprs.forEach { fname, fexpr ->
            val value = fexpr.eval(this)
            if (errors != null && (value.isInfinite() || value.isNaN())) {
                errors.incr(fname, 1)
            } else {
                this.features.put(fname, value)
            }
        }
    }

    fun forPassage(field: String, terms: List<String>): LTRDoc {
        val orig = fields[field] ?: error("Field $field does not actually exist!")
        val onlyField = LTRTokenizedDocField(field, terms, orig.tokenizer)
        return LTRDoc(name, HashMap(features), hashMapOf(onlyField.toEntry()), field)
    }

    companion object {
        fun create(id: String, fjson: Parameters, fields: Set<String>, index: IIndex): LTRDoc = create(id, fjson, fields, index.defaultField, index.tokenizer)
        fun create(id: String, fjson: Parameters, fields: Set<String>, defaultField: String, tokenizer: GenericTokenizer): LTRDoc {
            val ltrFields = HashMap<String, ILTRDocField>()
            val features = HashMap<String, Double>()

            fjson.keys.forEach { key ->
                if (fjson.isString(key)) {
                    val fieldText = fjson.getStr(key)
                    ltrFields.put(key, LTRDocField(key, fieldText, tokenizer))
                } else if(fjson.isDouble(key)) {
                    features.put("double-field-$key", fjson.getDouble(key))
                } else if(fjson.isLong(key)) {
                    features.put("long-field-$key", fjson.getLong(key).toDouble())
                } else {
                    println("Warning: Can't handle field: $key=${fjson[key]}")
                }
            }

            // Create empty fields as needed.
            fields
                    .filterNot { ltrFields.containsKey(it) }
                    .forEach { ltrFields[it] = LTREmptyDocField(it) }

            return LTRDoc(id, features, ltrFields, defaultField)
        }
    }
}

data class LTRDocScoringEnv(override val ltr: LTRDoc) : ScoringEnv(ltr.name.hashCode()) {
    override fun toString(): String {
        return "LTRDocScoringEnv(${ltr.name}, $doc)"
    }
}

class LTREvalSetupContext(override val env: RREnv) : EvalSetupContext {
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
    override fun count(env: ScoringEnv): Int = env.ltr.field(field).length
    override fun matches(env: ScoringEnv): Boolean = env.ltr.fields.contains(field)
}

data class LTRDocTerm(val field: String, val term: String): PositionsEvalNode, LTRDocFeatureNode() {
    var cache: Pair<String, IntArray>? = null
    override fun positions(env: ScoringEnv): PositionsIter {
        val doc = env.ltr
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
    override fun count(env: ScoringEnv): Int = env.ltr.field(field).count(term)
    override fun matches(env: ScoringEnv): Boolean = count(env) > 0
    override fun explain(env: ScoringEnv): Explanation = if (matches(env)) {
        Explanation.match(score(env).toFloat(), "${this.javaClass.simpleName} count=${count(env)} score=${score(env)} matches=${matches(env)} positions=${positions(env)}")
    } else {
        Explanation.noMatch("${this.javaClass.simpleName} count=${count(env)} score=${score(env)} matches=${matches(env)} positions=${positions(env)}")
    }
}

