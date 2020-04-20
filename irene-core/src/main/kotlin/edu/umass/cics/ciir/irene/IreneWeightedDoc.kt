package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.utils.WeightedForHeap

/**
 *
 * @author jfoley.
 */
data class IreneWeightedDoc(
        override val weight: Float,
        val doc: Int
) : WeightedForHeap {}
