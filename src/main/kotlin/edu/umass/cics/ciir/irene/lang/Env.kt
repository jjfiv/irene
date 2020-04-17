package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.IreneWeightedDoc
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.ltr.BagOfWords
import edu.umass.cics.ciir.irene.ltr.LTREvalSetupContext
import edu.umass.cics.ciir.irene.scoring.exprToEval

data class EnvConfig(
    var defaultField: String = "document",
    var defaultDirichletMu: Double = 1500.0,
    var defaultLinearSmoothingLambda: Double = 0.8,
    var defaultBM25b: Double = 0.75,
    var defaultBM25k: Double = 1.2,
    var absoluteDiscountingDelta: Double = 0.7,
    var estimateStats: String? = "min",
    var optimizeMovement: Boolean = true,
    var shareIterators: Boolean = true,
    var optimizeBM25: Boolean = false,
    var optimizeDirLog: Boolean = false,
    var indexedBigrams: Boolean = false,
    var prfStopwords: Set<String> = inqueryStop
)

abstract class RREnv {
    open var config = EnvConfig()

    val defaultField: String get() = config.defaultField
    val ltrContext = LTREvalSetupContext(this)
    fun makeLTRQuery(q: QExpr) = exprToEval(prepare(q), ltrContext)

    // nullable so it can be used to determine if this index has the given field.
    abstract fun fieldStats(field: String): CountStats
    abstract fun computeStats(q: QExpr): CountStats
    abstract fun getStats(term: String, field: String? =null): CountStats

    /**
     * Search via an atomic query.
     * @param q a query with no re-ranking components.
     * @param limit the depth with which to rank.
     * @return a list of ranked documents for a query.
     */
    abstract fun search(q: QExpr, limit: Int): List<IreneWeightedDoc>
    abstract fun lookupTerms(doc: Int, field: String): List<String>

    /** @return a list of internal identifiers */
    abstract fun lookupNames(docNames: Set<String>): List<Int>

    fun expandIfNecessary(q: QExpr): QExpr {
        val cache = HashMap<Int, BagOfWords>()
        var expanded = q;
        while (expanded.requiresPRF()) {
            expanded = doPRFStep(this, expanded, cache)
        }
        return expanded
    }

    fun prepare(q: QExpr): QExpr {
        var pq = simplify(expandIfNecessary(q))
                .map { reduceSingleChildren(it) }
        applyEnvironment(this, pq)
        try {
            analyzeDataNeededRecursive(pq)
        } catch (err: TypeCheckError) {
            System.err.println("$q")
            throw err
        }
        pq = insertStats(this, pq)

        // Optimize BM25:
        if (config.optimizeBM25) {
            // Only our system supports "lifting" the idf out of a BM25Expr.
            val bq = pq.map { c ->
                if (c is BM25Expr && !c.extractedIDF) {
                    val inner = c.child
                    val stats = c.stats
                    if (inner is TextExpr && stats != null) {
                        c.extractedIDF = true
                        val idf = Math.log(stats.dc / (stats.df + 0.5))
                        //println("IDF: ${idf} for term=${inner.text}")
                        c.weighted(idf)
                    } else {
                        c
                    }
                } else {
                    c
                }
            }
            return simplify(bq)
        } else {
            return simplify(pq)
        }
    }
}

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