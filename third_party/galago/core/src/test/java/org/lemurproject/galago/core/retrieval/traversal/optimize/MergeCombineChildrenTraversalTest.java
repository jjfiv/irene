/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class MergeCombineChildrenTraversalTest {

  @Test
  public void testNestedCombineMerger() throws Exception {
    String query = "#combine:3=0.7:2=0.3:1=0.8:0=0.2( #ow:1(#text:a() #text:b()) #text:b() #ow:1(#text:a() #text:b()) #text:d() )";
    Node result = StructuredQuery.parse(query);
    Node transformed = new MergeCombineChildrenTraversal().traverse(result, Parameters.create());
    assertEquals("#combine:0=0.5:1=0.8:2=0.7( #ow:1( #text:a() #text:b() ) #text:b() #text:d() )", transformed.toString());
  }
}

