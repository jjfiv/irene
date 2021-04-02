// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.geometric;

import org.junit.Test;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class GeometricIndexTest {
  @Test
  public void testProcessDocuments() throws Exception {
    PrintStream oldErr = System.err;
    PrintStream newErr = new PrintStream(new ByteArrayOutputStream());
    System.setErr(newErr);

    File shards = FileUtility.createTemporaryDirectory();
    try {
      Parameters p = Parameters.create();
      p.set("indexBlockSize", 50);
      p.set("shardDirectory", shards.getAbsolutePath());
      GeometricIndex index = new GeometricIndex(new FakeParameters(p));
      LocalRetrieval ret = new LocalRetrieval(index);

      for (int i = 0; i < 255; i++) {
        Document d = new Document();
        d.name = "DOC-" + i;
        d.text = "this is sample document " + i;
        d.terms = Arrays.asList(d.text.split(" "));
        d.tags = new ArrayList<Tag>();
        d.metadata = new HashMap<String,String>();

        index.process(d);
      }

      assertEquals(index.globalDocumentCount, 255);

      FieldStatistics cs = ret.getCollectionStatistics("#lengths:part=lengths()");
      assertEquals(cs.collectionLength, 1275);
      assertEquals(cs.documentCount, 255);
      assertEquals(cs.maxLength, 5);
      assertEquals(cs.minLength, 5);

      IndexPartStatistics stats = ret.getIndexPartStatistics("postings");
      assertEquals(stats.collectionLength, 1275);
      // these three are estimated as the max the set of shards
      assertEquals(stats.vocabCount, 154);
      assertEquals(stats.highestFrequency, 150);
      assertEquals(stats.highestDocumentCount, 150);

      stats = ret.getIndexPartStatistics("postings.krovetz");
      assertEquals(stats.collectionLength, 1275);
      // these three are estimated as the max of the set of shards
      assertEquals(stats.vocabCount, 154);
      assertEquals(stats.highestFrequency, 150);
      assertEquals(stats.highestDocumentCount, 150);

      ScoringContext sc = new ScoringContext();

      DataIterator<String> names = index.getNamesIterator();
      names.syncTo(99);
      sc.document = 99;
      assertEquals(names.data(sc), "DOC-" + 99);
      names.movePast(99);
      sc.document = names.currentCandidate();
      assertEquals(names.data(sc), "DOC-" + 100);

      LengthsIterator lengths = index.getLengthsIterator();
      lengths.syncTo(99);
      sc.document = 99;
      assertEquals(lengths.currentCandidate(), 99);
      assertEquals(lengths.length(sc), 5);
      lengths.movePast(99);
      sc.document = lengths.currentCandidate();
      assertEquals(lengths.currentCandidate(), 100);
      assertEquals(lengths.length(sc), 5);

      Node q1 = StructuredQuery.parse("#counts:sample:part=postings()");
      CountIterator ci1 = (CountIterator) index.getIterator(q1);
      assert ci1 != null;
      ci1.syncTo(99);
      sc.document = 99;
      assertEquals(ci1.currentCandidate(), 99);
      assertEquals(ci1.count(sc), 1);
      ci1.movePast(99);
      sc.document = ci1.currentCandidate();
      assertEquals(ci1.currentCandidate(), 100);
      assertEquals(ci1.count(sc), 1);

      Node q2 = StructuredQuery.parse("#counts:@/101/:part=postings()");
      CountIterator ci2 = (CountIterator) index.getIterator(q2);
      assertEquals(ci2.currentCandidate(), 101);
      sc.document = ci2.currentCandidate();
      assertEquals(ci2.count(sc), 1);
      ci2.movePast(101);
      assert (ci2.isDone());
      ci2.reset();
      assertEquals(ci2.currentCandidate(), 101);
      sc.document = ci2.currentCandidate();
      assertEquals(ci2.count(sc), 1);
      ci2.movePast(101);
      assert (ci2.isDone());

      index.close();

    } finally {
      FSUtil.deleteDirectory(shards);
      System.setErr(oldErr);
    }
  }

  @Test
  public void testRetrievalFunctions() throws Exception {
    File shards = FileUtility.createTemporaryDirectory();

    Random rnd = new Random();
    try {
      Parameters p = Parameters.create();
      p.set("indexBlockSize", 50);
      p.set("shardDirectory", shards.getAbsolutePath());
      p.set("requested", 10);
      GeometricIndex index = new GeometricIndex(new FakeParameters(p));
      LocalRetrieval ret = new LocalRetrieval(index);

      for (int i = 0; i < 255; i++) {

        Document d = new Document();
        d.name = "DOC-" + i;
        d.text = "this is sample document " + i;
        d.terms = Arrays.asList(d.text.split(" "));
        d.tags = new ArrayList<Tag>();
        d.metadata = new HashMap<String,String>();

        index.process(d);
        if (i > 0) {
          int j = rnd.nextInt(i);
          Node query = StructuredQuery.parse("sample " + j);
          query = ret.transformQuery(query, p);

          List<ScoredDocument> results = ret.executeQuery(query, p).scoredDocuments;
          assert (results.get(0).documentName.contains(Integer.toString(j)));
          }
      }
    } finally {
      FSUtil.deleteDirectory(shards);
      //System.setErr(oldErr);
    }
  }
}
