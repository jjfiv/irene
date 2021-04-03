// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.FixedSizeMinHeap;
import org.lemurproject.galago.utility.Parameters;

import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Performs straightforward document-at-a-time (daat) processing of a fully
 * annotated query, processing scores over documents.
 *
 * @author irmarc
 */
public class WorkingSetDocumentModel extends ProcessingModel {

  static final Logger logger = Logger.getLogger("WorkingSetDocModel");
  LocalRetrieval retrieval;
  Index index;

  public WorkingSetDocumentModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    // This model uses the simplest ScoringContext
    ScoringContext context = new ScoringContext();

    // There should be a whitelist to deal with
    List l = queryParams.getList("working");
    if (l == null) {
      throw new IllegalArgumentException("Parameters must contain a 'working' parameter specifying the working set");
    }

    if (l.isEmpty()) {
      throw new IllegalArgumentException("Working set may not be empty");
    }

    Class containedType = l.get(0).getClass();
    List<Long> whitelist;
    if (Long.class.isAssignableFrom(containedType)) {
      whitelist = (List<Long>) l;
    } else if (String.class.isAssignableFrom(containedType)) {
      whitelist = retrieval.getDocumentIds((List<String>) l);
          
      // check and print missing documents
      if(queryParams.get("warnMissingDocuments", true)) {
        for(long docId : whitelist){
          if(docId < 0){
            logger.warning("Document: " + docId + " does not exist in index: " + index.getIndexPath() +" IGNORING.");
          }
        }
      }
      
    } else {
      throw new IllegalArgumentException(
              String.format("Parameter 'working' must be a list of longs or a list of strings. Found type %s\n.",
              containedType.toString()));
    }
    Collections.sort(whitelist);

    // construct the query iterators
    ScoreIterator iterator =
            (ScoreIterator) retrieval.createIterator(queryParams, queryTree);
    int requested = queryParams.get("requested", 1000);
    boolean annotate = queryParams.get("annotate", false);

    // now there should be an iterator at the root of this tree
    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap<>(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

    for (long document : whitelist) {
      if (document < 0) {
        continue;
      }
      iterator.syncTo(document);
      context.document = document;

      // This context is shared among all scorers
      double score = iterator.score(context);
      if (requested < 0 || queue.size() < requested || queue.peek().score < score) {
        ScoredDocument scoredDocument = new ScoredDocument(document, score);
        if (annotate) {
          scoredDocument.annotation = iterator.getAnnotatedNode(context);
        }
        queue.offer(scoredDocument);
      }
    }
    return toReversedArray(queue);
  }
}
