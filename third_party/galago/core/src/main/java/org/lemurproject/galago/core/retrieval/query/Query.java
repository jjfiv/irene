/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.query;

import java.io.Serializable;
import org.lemurproject.galago.utility.Parameters;

/**
 * Query object
 * 
 *  Contains information for the processing of this query.
 * 
 * @author sjh
 */
public class Query implements Serializable {
  private static final long serialVersionUID = -8980110109622890397L;
  // query string, as input by user
  String rawInputQuery;

  // query node tree
  Node root; 
  
  // query parameters: extensible, serializable set of parameters to aid the processing of this query
  Parameters queryParameters;
  
}
