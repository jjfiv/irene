// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.compression.VByte;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Determines the class type of the input split, either based
 * on the "filetype" parameter passed in, or by guessing based on
 * the file path extension.
 *
 * (7/29/2012, irmarc): Refactored to be plug-and-play. External filetypes
 * may be added via the parameters.
 *
 * @author trevor, sjh, irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class UniversalParser extends StandardStep<DocumentSplit, Document> {

  private Counter documentCounter;
  private Parameters parameters;
  private static final Logger LOG = Logger.getLogger(UniversalParser.class.getSimpleName());
  private byte[] subCollCheck = "subcoll".getBytes();

  public UniversalParser(TupleFlowParameters parameters) {
    documentCounter = parameters.getCounter("Documents Parsed");
    this.parameters = parameters.getJSON();

    DocumentStreamParser.addExternalParsers(this.parameters);
  }

  @Override
  public void process(DocumentSplit split) throws IOException {

    long count = 0;
    long limit = Long.MAX_VALUE;
    if (split.startKey.length > 0) {
      if (CmpUtil.compare(subCollCheck, split.startKey) == 0) {
        limit = VByte.uncompressLong(split.endKey, 0);
      }
    }

    DocumentStreamParser parser = DocumentStreamParser.create(split, parameters);

    LOG.info("Processing split: "+split.fileName+ " with: "+parser.getClass().getName());

    // A parser is instantiated. Start producing documents for consumption
    // downstream.
    try {
      Document document;
      long fileDocumentCount = 0;
      while ((document = parser.nextDocument()) != null) {
        document.filePath = split.fileName;
        document.fileLocation = fileDocumentCount;
        fileDocumentCount++;

        document.fileId = split.fileId;
        document.totalFileCount = split.totalFileCount;
        processor.process(document);
        documentCounter.increment();
        count++;

        // Enforces limitations imposed by the endKey subcollection specifier.
        // See DocumentSource for details.
        if (count >= limit) {
          break;
        }

        if (count % 10000 == 0) {
          LOG.log(Level.WARNING, "Read " + count + " from split: " + split.fileName + " with "+parser.getClass());
        }
      }

      LOG.info("Processed " + count + " total in split: " + split.fileName+ " with "+parser.getClass());
    } finally {
      parser.close();
    }
  }
}
