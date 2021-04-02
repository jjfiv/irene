// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.scoring;

import org.lemurproject.galago.core.retrieval.RequiredParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

import java.io.IOException;

/**
 * Jelinek-Mercer smoothing node, applied over raw counts.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"nodeFrequency","collectionLength"})
@RequiredParameters(parameters = {"lambda"})
public class JelinekMercerScorer {

  double background;
  double lambda;

  public JelinekMercerScorer(NodeParameters parameters) throws IOException {

    lambda = parameters.get("lambda", 0.5D);
    long collectionLength = parameters.getLong("collectionLength");
    long collectionFrequency = parameters.getLong("nodeFrequency");
    background = (collectionFrequency > 0)
            ? (double) collectionFrequency / (double) collectionLength
            : 0.5 / (double) collectionLength;
  }

  public double score(int count, int length) {
    double foreground = (double) count / (double) length;
    return Math.log((lambda * foreground) + ((1 - lambda) * background));
  }
}
