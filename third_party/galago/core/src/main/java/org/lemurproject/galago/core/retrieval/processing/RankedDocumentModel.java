// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.FixedSizeMinHeap;
import org.lemurproject.galago.utility.Parameters;

/**
 * Performs straightforward document-at-a-time (daat) processing of a fully
 * annotated query, processing scores over documents.
 *
 * @author irmarc, sjh
 */
public class RankedDocumentModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;

  public RankedDocumentModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    // This model uses the simplest ScoringContext
    ScoringContext context = new ScoringContext();

    // Number of documents requested.
    int requested = queryParams.get("requested", 1000);
    boolean annotate = queryParams.get("annotate", false);

    // Maintain a queue of candidates
    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap<>(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

    // construct the iterators -- we use tree processing
    ScoreIterator iterator = (ScoreIterator) retrieval.createIterator(queryParams, queryTree);

    // now there should be an iterator at the root of this tree
    while (!iterator.isDone()) {
      long document = iterator.currentCandidate();

      // This context is shared among all scorers
      context.document = document;
      iterator.syncTo(document);
      if (iterator.hasMatch(context)) {
        double score = iterator.score(context);
        if (queue.size() < requested || queue.peek().score < score) {
          ScoredDocument scoredDocument = new ScoredDocument(document, score);
          if (annotate) {
            scoredDocument.annotation = iterator.getAnnotatedNode(context);
          }
          queue.offer(scoredDocument);
        }
      }
      iterator.movePast(document);
    }
    return toReversedArray(queue);
  }
}
