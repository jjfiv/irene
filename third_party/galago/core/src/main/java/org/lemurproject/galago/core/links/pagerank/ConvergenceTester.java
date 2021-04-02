/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import org.lemurproject.galago.core.types.PageRankScore;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author sjh
 */
public class ConvergenceTester implements ExNihiloSource<PageRankScore> {

  private final File convFile;
  private final String prevScoreStream;
  private final String currScoreStream;
  private final TupleFlowParameters tp;
  private final double delta;

  public ConvergenceTester(TupleFlowParameters p) {
    convFile = new File(p.getJSON().getString("convFile"));
    prevScoreStream = p.getJSON().getString("prevScoreStream");
    currScoreStream = p.getJSON().getString("currScoreStream");
    delta = p.getJSON().getDouble("delta");
    tp = p;
  }

  @Override
  public void run() throws IOException {
    TypeReader<PageRankScore> prevReader = tp.getTypeReader(prevScoreStream);
    TypeReader<PageRankScore> currReader = tp.getTypeReader(currScoreStream);

    PageRankScore prev = prevReader.read();
    PageRankScore curr = currReader.read();

    boolean converged = true;

    while (prev != null && curr != null) {
      // check difference
      if (prev.docName.equals(curr.docName)) {
        if (Math.abs(prev.score - curr.score) > delta) {
          converged = false;
          break;
        }
        prev = prevReader.read();
        curr = currReader.read();

      } else {
        // MAJOR PROBLEM -- misaligned document lists... we dropped one.
        System.err.println("DOCUMENT MISSING...: " + prev.docName + " - " + curr.docName + "\nAttempting to recover.");
        if (CmpUtil.compare(prev.docName, curr.docName) < 0) {
          prev = prevReader.read();
        } else {
          curr = currReader.read();
        }
      }
    }

    if (!converged) {
      try {
        convFile.createNewFile();
      } catch(IOException e){
        // may throw an error if multiple threads try simultaneously
        // in this case - not a problem.
      }
    }
  }

  @Override
  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    // Do nothing - we never call process.
  }

  public static void verify(TupleFlowParameters fullParameters, ErrorStore store) {
    Parameters parameters = fullParameters.getJSON();

    String[] requiredParameters = {"convFile", "prevScoreStream", "currScoreStream", "delta"};

    if (!Verification.requireParameters(requiredParameters, parameters, store)) {
      return;
    }

    Verification.verifyTypeReader(parameters.getString("prevScoreStream"), PageRankScore.class, new String[]{"+docName"}, fullParameters, store);
    Verification.verifyTypeReader(parameters.getString("currScoreStream"), PageRankScore.class, new String[]{"+docName"}, fullParameters, store);

  }
}
