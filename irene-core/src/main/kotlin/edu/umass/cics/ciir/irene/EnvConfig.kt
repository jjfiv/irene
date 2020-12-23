package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.utils.inqueryStop

/**
 *
 * @author jfoley.
 */
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
