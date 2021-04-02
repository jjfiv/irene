/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval.compare;

/**
 * Factory for creating QuerySetComparators
 *
 * @author sjh
 */
public class QuerySetComparatorFactory {

  /** @deprecated use create instead! */
  @Deprecated
  public static QuerySetComparator instance(String testName) {
    return create(testName);
  }
  public static QuerySetComparator create(String testName) {

    // sum metrics:
    if (testName.equals("baseline")) {
      return new Mean(testName, true);
    } else if (testName.equals("treatment")) {
      return new Mean(testName, false);
    } else if (testName.equalsIgnoreCase("baseBetter")) {
      return new CountBetter(testName, true);
    } else if (testName.equalsIgnoreCase("treatBetter")) {
      return new CountBetter(testName, false);
    } else if (testName.equals("equal")) {
      return new CountEqual(testName);
    } else if (testName.compareToIgnoreCase("ttest") == 0
            || testName.compareToIgnoreCase("pairedTTest") == 0) {
      return new PairedTTest(testName);
    } else if (testName.compareToIgnoreCase("signtest") == 0) {
      return new SignTest(testName);
    } else if (testName.compareToIgnoreCase("randomized") == 0) {
      return new RandomizedTest(testName);
    } else if (testName.startsWith("h-")){
      String[] parts = testName.split("-");
      assert(parts.length == 3): "Expecting QuerySetComparator named: h-<test>-<pvalue>";
      return new SupportHypothesis(testName, parts[1], Double.parseDouble(parts[2]));
    } else {
      throw new RuntimeException("Evaluation metric " + testName + " is unknown to QuerySetComparatorFactory.");
    }
  }

  public static QuerySetComparator instance(String testName, double boost) {
    // sum metrics:
    if (testName.compareToIgnoreCase("ttest") == 0
            || testName.compareToIgnoreCase("pairedTTest") == 0) {
      return new PairedTTest(testName, boost);
    } else if (testName.compareToIgnoreCase("signtest") == 0) {
      return new SignTest(testName, boost);
    } else if (testName.compareToIgnoreCase("randomized") == 0) {
      return new RandomizedTest(testName, boost);
    } else {
      throw new RuntimeException("Evaluation metric " + testName + " is unknown to QuerySetComparatorFactory.");
    }
  }
}
