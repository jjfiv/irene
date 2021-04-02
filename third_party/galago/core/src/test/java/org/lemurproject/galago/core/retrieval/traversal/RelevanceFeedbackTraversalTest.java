// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.prf.RelevanceModel1;
import org.lemurproject.galago.core.retrieval.prf.RelevanceModel3;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test is seriously a pain so all traversals that make use of 2 rounds of
 * retrieval should use the testing infrastructure set up here.
 *
 * If you want to print the various statistics, uncomment some of the print
 * calls below.
 *
 * TODO: Make stronger tests to increase confidence
 *
 * @author irmarc, sjh, dietz, dmf
 */
public class RelevanceFeedbackTraversalTest {

  File relsFile = null;
  File queryFile = null;
  File scoresFile = null;
  File trecCorpusFile = null;
  File corpusFile = null;
  File indexFile = null;

  // Build an index based on 10 short docs
  @Before
  public void setUp() throws Exception {
    File[] files = LocalRetrievalTest.make10DocIndex();
    trecCorpusFile = files[0];
    corpusFile = files[1];
    indexFile = files[2];
  }

  @Test
  public void testRelevanceModel1Traversal() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = Parameters.create();
    p.set("index", indexFile.getAbsolutePath());
    p.set("stemmedPostings", false);
    p.set("fbOrigWeight", 0.5);
    p.set("relevanceModel", RelevanceModel1.class.getName());
    p.set("rmwhitelist", "sentiwordlist.txt");
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.create(p);
    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval);

    Node parsedTree = StructuredQuery.parse("#rm:fbDocs=10:fbTerms=4( #dirichlet( #extents:jumped:part=postings() ) )");
    Node transformed = traversal.traverse(parsedTree, Parameters.create());
    // truth data
    StringBuilder correct = new StringBuilder();
    /* No sentiwordlist.txt
    correct.append("#combine:0=0.05001660577881102:1=0.05001660577881102:2=0.04165282851765748:3=0.04165282851765748( ");
    correct.append("#text:sample() ");
    correct.append("#text:ugly() ");
    correct.append("#text:cat() ");
    correct.append("#text:moon() )");
  */
    assertEquals(transformed.getNodeParameters().get("0", -1.0), 0.05001, 0.00001);
    assertEquals(transformed.getNodeParameters().get("1", -1.0), 0.04165, 0.00001);
    assertEquals("text", transformed.getChild(0).getOperator());
    assertEquals("ugly", transformed.getChild(0).getDefaultParameter());
    assertEquals("text", transformed.getChild(1).getOperator());
    assertEquals("moon", transformed.getChild(1).getDefaultParameter());
    //correct.append("#combine:0=0.05001660577881102:1=0.04165282851765748( ");
    //correct.append("#text:ugly() ");
    //correct.append("#text:moon() )");

    retrieval.close();
  }

  @Test
  public void testRelevanceModel3Traversal() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = Parameters.create();
    p.set("index", indexFile.getAbsolutePath());
    p.set("stemmedPostings", false);
    p.set("fbOrigWeight", 0.9);
    p.set("relevanceModel", RelevanceModel3.class.getName());
    p.set("rmwhitelist", "sentiwordlist.txt");
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.create(p);
    RelevanceModelTraversal traversal = new RelevanceModelTraversal(retrieval);

    Node parsedTree = StructuredQuery.parse("#rm:fbDocs=10:fbTerms=4( #dirichlet( #extents:jumped:part=postings() ) )");
    Node transformed = traversal.traverse(parsedTree, Parameters.create());
    // truth data
    StringBuilder correct = new StringBuilder();

    assertEquals("combine", transformed.getChild(0).getOperator());
    assertEquals("dirichlet", transformed.getChild(0).getChild(0).getOperator());

    // top level 0.9 original, 0.1 new
    assertEquals(transformed.getNodeParameters().get("0", -1.0), 0.9, 0.00001);
    assertEquals(transformed.getNodeParameters().get("1", -1.0), 0.1, 0.00001);

    // inside new:
    assertEquals(transformed.getChild(1).getNodeParameters().get("0", -1.0), 0.05001, 0.00001);
    assertEquals(transformed.getChild(1).getNodeParameters().get("1", -1.0), 0.04165, 0.00001);

    assertEquals("text", transformed.getChild(1).getChild(0).getOperator());
    assertEquals("ugly", transformed.getChild(1).getChild(0).getDefaultParameter());
    assertEquals("text", transformed.getChild(1).getChild(1).getOperator());
    assertEquals("moon", transformed.getChild(1).getChild(1).getDefaultParameter());

    //correct.append("#combine:0=0.9:1=0.09999999999999998( #combine:fbDocs=10:fbTerms=4( #dirichlet( #extents:jumped:part=postings() ) ) ");
    //correct.append("#combine:0=0.05001660577881102:1=0.04165282851765748( #text:ugly() #text:moon() ) )");
    
    //System.err.println(transformed.toString());
    //System.err.println(correct.toString());

    //assertEquals(correct.toString(), transformed.toString());
 
    retrieval.close();
  }

  @Test
  public void testRelevanceModelEmptyTraversal() throws Exception {
    // Create a retrieval object for use by the traversal
    Parameters p = Parameters.create();
    p.set("index", indexFile.getAbsolutePath());
    p.set("stemmedPostings", false);
    p.set("fbOrigWeight", 0.9);
    p.set("relevanceModel", RelevanceModel3.class.getName());
    p.set("rmwhitelist", "sentiwordlist.txt");
    LocalRetrieval retrieval = (LocalRetrieval) RetrievalFactory.create(p);
    
    Node parsedTree = StructuredQuery.parse("#rm:fbDocs=10:fbTerms=4( neverawordinedgewise )");
    Node transformed = retrieval.transformQuery(parsedTree, p);
    // truth data
    StringBuilder correct = new StringBuilder();
    correct.append("#combine:fbDocs=10:fbTerms=4:w=1.0( ")
           .append("#dirichlet:collectionLength=70:maximumCount=0:nodeFrequency=0:w=1.0( #lengths:document:part=lengths() #counts:neverawordinedgewise:part=postings() ) )");
        
    System.err.println(transformed.toString());
    System.err.println(correct.toString());

    assertEquals(correct.toString(), transformed.toString());
 
    List <ScoredDocument> results = retrieval.executeQuery(transformed).scoredDocuments;        
    assertTrue(results.isEmpty());
   
    retrieval.close();
  }

  @After
  public void tearDown() throws Exception {
    if (relsFile != null) {
      relsFile.delete();
    }
    if (queryFile != null) {
      queryFile.delete();
    }
    if (scoresFile != null) {
      scoresFile.delete();
    }
    if (trecCorpusFile != null) {
      trecCorpusFile.delete();
    }
    if (corpusFile != null) {
      FSUtil.deleteDirectory(corpusFile);
    }
    if (indexFile != null) {
      FSUtil.deleteDirectory(indexFile);
    }
  }
}
