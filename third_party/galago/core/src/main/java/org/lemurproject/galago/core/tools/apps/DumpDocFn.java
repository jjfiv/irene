/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class DumpDocFn extends AppFunction {

  @Override
  public String getName() {
    return "doc";
  }

  @Override
  public String getHelpString() {
    return "galago doc [--help] --index=<index> --id=<identifier> [format parameters]\n\n"
            + "  Prints the full text of the document named by <identifier>.\n"
            + "  The document is retrieved from a Corpus file named corpus.\n"
            + "  <index> must contain a corpus structure.\n"
            + "  [format parameters] :\n"
            + "\t --text=[true|false]\n"
            + "\t --metadata=[true|false]\n"
            + "\t --tokenize=[true|false]\n"
            ;
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (p.get("help", false)) {
      output.println(getHelpString());
      return;
    }
    String indexPath = p.getString("index");
    String identifier = p.getAsString("id");
    
    DocumentComponents dc = new DocumentComponents(p);
    
    Retrieval r = RetrievalFactory.instance(indexPath, Parameters.create());
    assert r.getAvailableParts().containsKey("corpus") : "Index does not contain a corpus part.";
    Document document = r.getDocument(identifier, dc);
    if (document != null) {
      output.println(document.toString());
    } else {
      output.println("Document " + identifier + " does not exist in index " + indexPath + ".");
    }
  }
}
