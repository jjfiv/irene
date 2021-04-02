// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author irmarc
 */
public class SequentialDependenceTraversalTest {

  File indexPath;

  @Before
  public void setUp() throws FileNotFoundException, IOException, IncompatibleProcessorException {
    indexPath = LocalRetrievalTest.makeIndex();
  }

  @After
  public void tearDown() throws IOException {
    FSUtil.deleteDirectory(indexPath);
  }

  @Test
  public void testTraversal() throws Exception {
    DiskIndex index = new DiskIndex(indexPath.getAbsolutePath());
    Parameters p = Parameters.create();
    LocalRetrieval retrieval = new LocalRetrieval(index, p);
    SequentialDependenceTraversal traversal = new SequentialDependenceTraversal(retrieval);
    Node tree = StructuredQuery.parse("#sdm( cat dog rat )");
    StringBuilder transformed = new StringBuilder();
    transformed.append("#combine:0=0.8:1=0.15:2=0.05( ");
    transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
    transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
    transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
    Node result = traversal.traverse(tree, p);

    assertEquals(transformed.toString(), result.toString());

    // now change weights
    p = Parameters.create();
    p.set("uniw", 0.75);
    p.set("odw", 0.10);
    p.set("uww", 0.15);
    retrieval = new LocalRetrieval(index, p);
    traversal = new SequentialDependenceTraversal(retrieval);
    tree = StructuredQuery.parse("#sdm( cat dog rat )");
    transformed = new StringBuilder();
    transformed.append("#combine:0=0.75:1=0.1:2=0.15( ");
    transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
    transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
    transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
    result = traversal.traverse(tree, p);

    assertEquals(transformed.toString(), result.toString());

    // now change weights via the operator
    tree = StructuredQuery.parse("#sdm:uniw=0.55:odw=0.27:uww=0.18( cat dog rat )");
    transformed = new StringBuilder();
    transformed.append("#combine:0=0.55:1=0.27:2=0.18( ");
    transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
    transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) ) ");
    transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) ) )");
     result = traversal.traverse(tree, p);

    assertEquals(transformed.toString(), result.toString());

    // now change the window size param
    p = Parameters.create();
    p.set("uniw", 0.75);
    p.set("odw", 0.10);
    p.set("uww", 0.15);
    p.set("windowLimit", 3);
    retrieval = new LocalRetrieval(index, p);
    traversal = new SequentialDependenceTraversal(retrieval);
    tree = StructuredQuery.parse("#sdm( cat dog rat )");
    transformed = new StringBuilder();
    transformed.append("#combine:0=0.75:1=0.1:2=0.15( ");
    transformed.append("#combine( #text:cat() #text:dog() #text:rat() ) ");
    transformed.append("#combine( #ordered:1( #text:cat() #text:dog() ) #ordered:1( #text:dog() #text:rat() ) #ordered:1( #text:cat() #text:dog() #text:rat() ) ) ");
    transformed.append("#combine( #unordered:8( #text:cat() #text:dog() ) #unordered:8( #text:dog() #text:rat() ) #unordered:12( #text:cat() #text:dog() #text:rat() ) ) )");
    result = traversal.traverse(tree, p);

    assertEquals(transformed.toString(), result.toString());

  }
}
