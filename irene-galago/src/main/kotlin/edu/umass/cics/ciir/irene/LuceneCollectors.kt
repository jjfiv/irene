package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.lang.MultiExpr
import edu.umass.cics.ciir.irene.scoring.IreneQueryScorer
import edu.umass.cics.ciir.irene.scoring.MultiEvalNode
import edu.umass.cics.ciir.irene.scoring.ScoringEnv
import edu.umass.cics.ciir.irene.utils.ReservoirSampler
import edu.umass.cics.ciir.irene.utils.ScoringHeap
import edu.umass.cics.ciir.irene.utils.WeightedForHeap
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.*
import org.roaringbitmap.RoaringBitmap
import java.util.*
import kotlin.collections.ArrayList

/**
 * @author jfoley
 */




