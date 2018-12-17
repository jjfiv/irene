package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.ltr.LTREvalSetupContext
import edu.umass.cics.ciir.irene.scoring.exprToEval

abstract class RREnv {
    open var defaultField = "document"
    open var defaultDirichletMu = 1500.0
    open var defaultLinearSmoothingLambda = 0.8;
    open var defaultBM25b = 0.75
    open var defaultBM25k = 1.2
    open var absoluteDiscountingDelta = 0.7
    open var estimateStats: String? = "min"
    open var optimizeMovement = true
    open var shareIterators: Boolean = true
    open var optimizeBM25 = false
    open var optimizeDirLog = false
    open var indexedBigrams = false

    val ltrContext = LTREvalSetupContext(this)
    fun makeLTRQuery(q: QExpr) = exprToEval(prepare(q), ltrContext)

    // nullable so it can be used to determine if this index has the given field.
    abstract fun fieldStats(field: String): CountStats
    abstract fun computeStats(q: QExpr): CountStats
    abstract fun getStats(term: String, field: String? =null): CountStats

    /** @return a list of internal identifiers */
    abstract fun lookupNames(docNames: Set<String>): List<Int>

    fun prepare(q: QExpr): QExpr {
        var pq =
                simplify(q)
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
        if (optimizeBM25) {
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