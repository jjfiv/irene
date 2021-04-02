/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.utility.Parameters;

/**
 * Factory for QueryEvaluators
 *
 * @author sjh
 */
public class QueryEvaluatorFactory {

  /** @deprecated use create instead! */
  @Deprecated
  public static QueryEvaluator instance(String metric, Parameters p) {
    return create(metric, p);
  }
  /** Static functions allow the creation of QueryEvaluators based on strings. */
  public static QueryEvaluator create(String metric, Parameters p) {

    String lowerMetric = metric.toLowerCase();

    // these metrics may not be parametized
    switch (lowerMetric) {
      case "num_ret":
        return new CountRetrieved(metric);
      case "num_rel":
        return new CountRelevant(metric);
      case "jmap":
        return new JudgedAveragePrecision(metric);
      case "map":
      case "ap":
      case "averageprecision":
        return new AveragePrecision(metric);
      case "r-prec":
      case "rprecision":
        return new RPrecision(metric);
      case "bpref":
        return new BinaryPreference(metric);
      case "recip_rank":
        return new ReciprocalRank(metric);
      case "p":
        return new Precision(metric);
      case "r":
        return new Recall(metric);
      case "ndcg":
        return new NormalizedDiscountedCumulativeGain(metric);
      case "err":
        return new ExpectedReciprocalRank(metric);
    }

      // these may be parametized (e.g. P5, R10, ndcg20, ...)

    if (lowerMetric.startsWith("p")) {
      int documentLimit = Integer.parseInt(lowerMetric.replace("p", ""));
      return new Precision(metric, documentLimit);
    } else if (lowerMetric.startsWith("r")) {
      int documentLimit = Integer.parseInt(lowerMetric.replace("r", ""));
      return new Recall(metric, documentLimit);
    } else if (lowerMetric.startsWith("ndcg")) {
      int documentLimit = Integer.parseInt(lowerMetric.replace("ndcg", ""));
      return new NormalizedDiscountedCumulativeGain(metric, documentLimit);
    } else if (lowerMetric.startsWith("err")) {
      int documentLimit = Integer.parseInt(lowerMetric.replace("err", ""));
      return new ExpectedReciprocalRank(metric, documentLimit);
      
      // these are parameterized with @ symbols... :/
    } else if (lowerMetric.startsWith("num_rel_ret")) {
      return new CountRelevantRetrieved(metric);
    } else if (lowerMetric.startsWith("num_unjug_ret")) {
      return new CountUnjudged(metric);
    } else if (lowerMetric.startsWith("frac_unjug_ret")) {
      return new FractionUnjudged(metric);
    } else if (lowerMetric.startsWith("muap")) {
      return UnjudgedAveragePrecision.create(lowerMetric);
    } else if (lowerMetric.startsWith("mdfa")) {
        int falseAlarmRate = Integer.parseInt(lowerMetric.replace("mdfa", ""));
        double rate = falseAlarmRate / (double) 100;
        long collectionSize = p.getLong("collectionSize");
        return new MissDetectionFalseAlarm(metric, collectionSize, rate);
      
    // otherwise we don't know which metric to use.
    } else {
      throw new RuntimeException("Evaluation metric " + metric + " is unknown to QueryEvaluatorFactory.");
    }
  }
}
