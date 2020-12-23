package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.ltr.BagOfWords
import edu.umass.cics.ciir.irene.utils.inqueryStop

/**
 *
 * @author jfoley.
 */
fun doPRFStep(env: RREnv, query: QExpr, cache: HashMap<Int, BagOfWords>): QExpr {
    return query.map { q ->
        when(q) {
            is RM3Expr -> {
                if (q.child.requiresPRF()) {
                    error("QExpr.map should be bottom-up. Did you add a new PRF-QExpr?")
                }
                RelevanceExpansionQ(env,
                        q.child, q.fbDocs, q.origWeight, q.fbTerms,
                        q.field ?: env.defaultField,
                        if (q.stopwords) { env.config.prfStopwords } else { emptySet() },
                        cache) ?: error("PRF had no matches: $q")
            }
            else -> q
        }
    }
}

fun RelevanceExpansionQ(env: RREnv, firstPass: QExpr, depth: Int=50, origWeight: Double = 0.3, numTerms: Int=100, expansionField: String?=null, stopwords: Set<String> = inqueryStop, cache: HashMap<Int, BagOfWords> = HashMap()): QExpr? {
    val rm = ComputeRelevanceModel(env, firstPass, depth, expansionField, stopwords, cache) ?: return null
    val expQ = rm.toQExpr(numTerms)
    return SumExpr(firstPass.weighted(origWeight), expQ.weighted(1.0 - origWeight))
}
