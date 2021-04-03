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
public class FlattenWindowTraversalTest {
  @Test
  public void testNestedWindowRewrite() throws Exception {
    String query = "#uw:5( #od:1(#text:a() #text:b()) )";
    Node result = StructuredQuery.parse(query);
    Node transformed = new FlattenWindowTraversal().traverse(result, Parameters.create());
    assertEquals("#od:1( #text:a() #text:b() )", transformed.toString());
  }
}
