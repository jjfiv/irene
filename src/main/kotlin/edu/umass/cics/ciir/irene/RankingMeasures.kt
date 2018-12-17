package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.utils.safeDiv
import java.util.Comparator;

data class PredTruth(val truth: Boolean, val prediction: Double) {
  constructor(input: Pair<Boolean, Double>) : this(input.first, input.second)
}

object RankingMeasures {
  val scoreTiesWorstCase = Comparator<PredTruth> { o1, o2 ->
    // highest scores first:
    val score = -java.lang.Double.compare(o1.prediction, o2.prediction);
    if(score != 0) return@Comparator score;
    // false, then true:
    java.lang.Boolean.compare(o1.truth, o2.truth)
  }

  val scoreTiesBestCase = Comparator<PredTruth> { o1, o2 ->
    // highest scores first:
    val score = -java.lang.Double.compare(o1.prediction, o2.prediction);
    if(score != 0) return@Comparator score;
    // true, then false:
    -java.lang.Boolean.compare(o1.truth, o2.truth);
  }
  enum class TieBreaking {
    WORST_CASE, // if we assume all ties are broken in the worst way (positives lowest in ranking)
    BEST_CASE // if we assume all ties are broken in the best way (positives highest in ranking)
    ;

    fun getComparator(): Comparator<PredTruth> = when (this) {
      WORST_CASE -> scoreTiesWorstCase
      BEST_CASE -> scoreTiesBestCase
    }
  }


  fun computeRR(inPoints: List<PredTruth>): Double {
    if(inPoints.isEmpty()) {
      return 0.0;
    }
    val points = inPoints.toMutableList()
    // order by prediction confidence:
    points.sortWith(TieBreaking.WORST_CASE.getComparator())

    for (i in points.indices) {
      if (points[i].truth) {
        return 1.0 / (i+1).toDouble()
      }
    }
    return 0.0
  }

  fun computeAUC(inPoints: List<PredTruth>, what: TieBreaking = TieBreaking.WORST_CASE): Double {
    if(inPoints.size < 2) {
      throw UnsupportedOperationException("Cannot compute AUC without at least two points.");
    }
    val points = inPoints.toMutableList()
    // order by prediction confidence:
    points.sortWith(what.getComparator())

    val true_pos_rate = DoubleArray(points.size)
    val false_pos_rate = DoubleArray(points.size)

    var total_true_pos = 0;
    var total_false_pos = 0;
    for (point in points) {
      if(point.truth)
        total_true_pos++;
      else
        total_false_pos++;
    }


    // walk the ranked list and build graph:
    var true_pos = 0;
    var false_pos = 0;
    for (i in 0 .. points.size) {

      if(points[i].truth) {
        true_pos++;
      } else {
        false_pos++;
      }
      val N = i+1;
      true_pos_rate[i] = true_pos / total_true_pos.toDouble();
      false_pos_rate[i] = false_pos / total_false_pos.toDouble();
    }

    // estimate area under the curve using the trapezoidal rule, as SciKitLearn does via np.trapz
    val y = true_pos_rate;
    val x = false_pos_rate;

    val lim = x.size-1;
    var sum = 0.0;
    for (i in 0..lim) {
      val dx = x[i+1]-x[i];
      val midy = (y[i+1]+y[i]); // div 2 hoisted to end
      sum += dx*midy;
    }
    return sum / 2.0;
  }

  fun computeRecall(data: List<PredTruth>, true_positives: Int?, cutoff: Int = data.size): Double {
    if (data.size == 0) {
      return 0.0
    }
    val N = if (true_positives != null) {
      true_positives
    } else {
      data.count { it.truth }
    }

    if (N == 0) {
      return 0.0;
    }

    val points = data.toMutableList()
    // order by prediction confidence:
    points.sortWith(TieBreaking.WORST_CASE.getComparator())
    var correct = 0
    for (i in points.indices) {
      if (i >= cutoff) break
      if(data[i].truth) {
        correct++;
      }
    }

    return safeDiv(correct, N);
  }

  fun computePrec(data: List<PredTruth>, cutoff: Int = data.size): Double {
    if (data.size == 0) return 0.0

    val points = data.toMutableList()
    // order by prediction confidence:
    points.sortWith(TieBreaking.WORST_CASE.getComparator())
    val N = Math.min(cutoff, points.size)
    var correct = 0
    for (i in points.indices) {
      if (i >= cutoff) break
      if(points[i].truth) {
        correct++;
      }
    }
    return safeDiv(correct, N);
  }

  fun computeAP(data: List<PredTruth>, kind: TieBreaking = TieBreaking.WORST_CASE, numRelevant: Int? = null): Double {
    val NR = if (numRelevant != null) {
      numRelevant
    } else {
      data.count { it.truth }
    }

    // if there are no relevant documents,
    // the average is artificially defined as zero, to mimic trec_eval
    // Really, the output is NaN, or the query should be ignored.
    if(NR == 0) return 0.0;
    val points = data.toMutableList()
    points.sortWith(kind.getComparator())

    var sumPrecision = 0;
    var recallPointCount = 0;

    for (i in points.indices) {
      if(points[i].truth) {
        val rank = i + 1;
        recallPointCount++;
        sumPrecision += recallPointCount / rank;
      }
    }

    return sumPrecision / NR.toDouble();
  }
}
