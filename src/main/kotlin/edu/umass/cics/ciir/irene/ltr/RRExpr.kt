package edu.umass.cics.ciir.irene.ltr

import edu.umass.cics.ciir.irene.utils.computeEntropy
import edu.umass.cics.ciir.irene.utils.mean
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.irene.scoring.LTRDocScoringEnv
import edu.umass.cics.ciir.irene.scoring.QueryEvalNode
import org.apache.lucene.search.Explanation

fun QExpr.toRRExpr(env: RREnv): RRExpr {
    return RREvalNodeExpr(env, this.deepCopy())
}

sealed class RRExpr(val env: RREnv) {
    abstract fun eval(doc: LTRDoc): Double
    open fun explain(doc: LTRDoc): Explanation = Explanation.match(eval(doc).toFloat(), "RRExpr:${this.javaClass.simpleName}")
}

class RREvalNodeExpr(env: RREnv, val node: QueryEvalNode) : RRLeafExpr(env) {
    constructor(env: RREnv, q: QExpr) : this(env, env.makeLTRQuery(q))
    override fun eval(doc: LTRDoc): Double {
        val scoreEnv = LTRDocScoringEnv(doc)
        return node.score(scoreEnv)
    }

    override fun explain(doc: LTRDoc): Explanation {
        val scoreEnv = LTRDocScoringEnv(doc)
        return node.explain(scoreEnv)
    }
}

sealed class RRCCExpr(env: RREnv, val exprs: List<RRExpr>): RRExpr(env) {
    init {
        assert(exprs.size > 0)
    }
}
class RRSum(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    override fun eval(doc: LTRDoc): Double = exprs.sumByDouble { it.eval(doc) }
    override fun toString(): String = "RRSum($exprs)"
}
class RRMean(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    val N = exprs.size.toDouble()
    override fun eval(doc: LTRDoc): Double = exprs.sumByDouble { it.eval(doc) } / N
}
class RRMult(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    override fun eval(doc: LTRDoc): Double {
        var prod = 1.0
        exprs.forEach { prod *= it.eval(doc) }
        return prod
    }
}

sealed class RRLeafExpr(env: RREnv) : RRExpr(env)
class RRFeature(env: RREnv, val name: String): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        return doc.features[name]!!
    }
}
class RRConst(env: RREnv, val value: Double) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double = value
}

class RRAvgWordLength(env: RREnv, val field: String = env.defaultField) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double = doc.terms(field).map { it.length }.mean()
}

class RREntropy(env: RREnv, val field: String = env.defaultField) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double = doc.terms(field).computeEntropy()
}

/**
 * This calculates the number of unique terms in a document divided by its length. Similar to how noisy the document is, so I called it hte DocInfoQuotient. This is used in Absolute Discounting.
 */
class RRDocInfoQuotient(env: RREnv, val field: String = env.defaultField): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        val field = doc.field(field)
        return field.uniqTerms.toDouble() / Math.max(1.0,field.length.toDouble())
    }
}

class RRDocLength(env: RREnv, val field: String = env.defaultField) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double = doc.field(field).length.toDouble()
}

// Return the index of a term in a document as a fraction: for news, this should be similar to importance. A query term at the first position will receive a 1.0; halfway through the document will receive a 0.5.
class RRTermPosition(env: RREnv, val term: String, val field: String = env.defaultField): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        val field = doc.field(field)
        val length = field.length.toDouble()
        val pos = field.terms.indexOf(term).toDouble()
        if (pos < 0) {
            return 0.0
        } else {
            return 1.0 - (pos / length)
        }
    }
}

class RRJaccardSimilarity(env: RREnv, val target: Set<String>, val field: String = env.defaultField, val empty: Double=0.5): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        if (target.isEmpty()) return empty
        val field = doc.field(field)
        val uniq = field.termSet
        if (uniq.isEmpty()) return empty
        val overlap = (uniq intersect target).size.toDouble()
        val domain = (uniq union target).size.toDouble()
        return overlap / domain
    }
}

class RRLogLogisticTFScore(env: RREnv, val term: String, val field: String = env.defaultField, val c: Double = 1.0) : RRLeafExpr(env) {
    companion object {
        val OriginalPaperCValues = arrayListOf<Number>(0.5,0.75,1,2,3,4,5,6,7,8,9).map { it.toDouble() }
    }
    val stats = env.getStats(term, field)
    val avgl = stats.avgDL()
    val lambda_w = stats.binaryProbability()
    init {
        assert(stats.cf > 0) { "Term ``$term'' in field ``$field'' must actually occur in the collection for this feature to make sense." }
    }

    override fun eval(doc: LTRDoc): Double {
        val field = doc.field(field)
        val tf = field.count(term)
        val lengthRatio = avgl / field.length.toDouble()
        val t = tf * Math.log(1.0 + c * lengthRatio)
        return Math.log((t + lambda_w) / lambda_w)
    }
}

