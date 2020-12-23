package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.EnvConfig
import edu.umass.cics.ciir.irene.IreneWeightedDoc
import edu.umass.cics.ciir.irene.ltr.BagOfWords

/**
 *
 * @author jfoley.
 */
abstract class RREnv {
    open var config = EnvConfig()

    val defaultField: String get() = config.defaultField
    //val ltrContext = LTREvalSetupContext(this)
    //fun makeLTRQuery(q: QExpr) = exprToEval(prepare(q), ltrContext)

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
