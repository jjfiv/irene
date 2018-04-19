package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.utils.forAllPairs
import edu.umass.cics.ciir.irene.utils.forEachSeqPair
import edu.umass.cics.ciir.irene.utils.mapEachSeqPair

/**
 *
 * @author jfoley.
 */
fun SmartStop(terms: List<String>, stopwords: Set<String>): List<String> {
    val nonStop = terms.filter { !stopwords.contains(it) }
    if (nonStop.isEmpty()) {
        return terms
    }
    return nonStop
}

fun phraseQuery(terms: List<String>, field: String? = null, statsField: String? = null) = when {
    terms.isEmpty() -> NeverMatchLeaf
    terms.size == 1 -> TextExpr(terms[0], field, statsField)
    else -> OrderedWindowExpr(terms.map { TextExpr(it, field, statsField) })
}

// Do we find the exact phrasing, and do we
fun generateExactMatchQuery(qterms: List<String>, field: String?=null, statsField: String?=null): QExpr {
    return AndExpr(listOf(phraseQuery(qterms, field, statsField), CountEqualsExpr(LengthsExpr(field), qterms.size)))
}

// Easy "model"-based constructor.
fun QueryLikelihood(terms: List<String>, field: String?=null, statsField: String?=null, mu: Double? = null): QExpr {
    return UnigramRetrievalModel(terms, { DirQLExpr(it, mu) }, field, statsField)
}
fun BM25Model(terms: List<String>, field: String?=null, statsField: String?=null, b: Double? = null, k: Double? = null): QExpr {
    return UnigramRetrievalModel(terms, { BM25Expr(it, b, k) }, field, statsField)
}
fun UnigramRetrievalModel(terms: List<String>, scorer: (TextExpr)-> QExpr, field: String?=null, statsField: String?=null): QExpr {
    return MeanExpr(terms.map { scorer(TextExpr(it, field, statsField)) })
}

fun SequentialDependenceModel(terms: List<String>,
                              field: String?=null,
                              statsField: String?=null,
                              stopwords: Set<String> =emptySet(),
                              uniW: Double = 0.8, odW: Double = 0.15, uwW: Double = 0.05,
                              odStep: Int=1, uwWidth:Int=8,
                              fullProx: Double? = null, fullProxWidth:Int=12,
                              makeScorer: (QExpr)-> QExpr = { DirQLExpr(it) },
        // SumExpr to match Galago, probably want MeanExpr if you're nesting...
                              outerExpr: (List<QExpr>) -> QExpr = { SumExpr(it) }): QExpr {
    if (terms.isEmpty()) throw IllegalStateException("Empty SDM")
    if (terms.size == 1) {
        return makeScorer(TextExpr(terms[0], field, statsField))
    }

    val nonStop = terms.filterNot { stopwords.contains(it) }
    val bestTerms = (if (nonStop.isNotEmpty()) { nonStop } else terms)
    val unigrams: List<QExpr> = bestTerms
            .map { makeScorer(TextExpr(it, field, statsField)) }

    val smartPairs = terms.mapEachSeqPair { lhs, rhs ->
        val tokens = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        val bothStop = terms.all { stopwords.contains(it) }
        Pair(bothStop, tokens)
    }

    val usePairs = if (smartPairs.all { it.first }) {
        // all stopwords, generate dependencies
        smartPairs.map { it.second }
    } else {
        // only pairs that aren't both stopwords.
        smartPairs.filter { !it.first }.map { it.second }
    }

    val bigrams = ArrayList<QExpr>()
    val ubigrams = ArrayList<QExpr>()
    usePairs.forEach { ts ->
        bigrams.add(makeScorer(OrderedWindowExpr(ts, odStep)))
    }
    bestTerms.forEachSeqPair { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        ubigrams.add(makeScorer(UnorderedWindowExpr(ts, uwWidth)))
    }

    val exprs = arrayListOf(
            MeanExpr(unigrams).weighted(uniW),
            MeanExpr(bigrams).weighted(odW),
            MeanExpr(ubigrams).weighted(uwW))

    if (fullProx != null) {
        val fullProxTerms = if (bestTerms.size >= 2) bestTerms else terms
        exprs.add(makeScorer(UnorderedWindowExpr(fullProxTerms.map { TextExpr(it, field, statsField) }, fullProxWidth)).weighted(fullProx))
    }

    return outerExpr(exprs)
}

fun FullDependenceModel(terms: List<String>, field: String?=null, statsField: String? = null, stopwords: Set<String> =emptySet(), uniW: Double = 0.8, odW: Double = 0.15, uwW: Double = 0.05, odStep: Int=1, uwWidth:Int=8, fullProx: Double? = null, fullProxWidth:Int=12, makeScorer: (QExpr)-> QExpr = { DirQLExpr(it) }): QExpr {
    if (terms.isEmpty()) throw IllegalStateException("Empty FDM")
    if (terms.size == 1) {
        return makeScorer(TextExpr(terms[0], field, statsField))
    }

    val nonStop = terms.filterNot { stopwords.contains(it) }
    val bestTerms = (if (nonStop.isNotEmpty()) { nonStop } else terms)
    val unigrams: List<QExpr> = bestTerms
            .map { makeScorer(TextExpr(it, field, statsField)) }

    val bigrams = ArrayList<QExpr>()
    val ubigrams = ArrayList<QExpr>()
    terms.forAllPairs { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        bigrams.add(makeScorer(OrderedWindowExpr(ts, odStep)))
    }
    bestTerms.forAllPairs { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        ubigrams.add(makeScorer(UnorderedWindowExpr(ts, uwWidth)))
    }

    val exprs = arrayListOf(
            MeanExpr(unigrams).weighted(uniW),
            MeanExpr(bigrams).weighted(odW),
            MeanExpr(ubigrams).weighted(uwW))

    if (fullProx != null) {
        val fullProxTerms = if (bestTerms.size >= 2) bestTerms else terms
        exprs.add(makeScorer(UnorderedWindowExpr(fullProxTerms.map { TextExpr(it, field, statsField) }, fullProxWidth)).weighted(fullProx))
    }

    return SumExpr(exprs)
}

