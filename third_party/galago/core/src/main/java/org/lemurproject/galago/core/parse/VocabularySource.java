// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.btree.format.BTreeFactory;
import org.lemurproject.galago.tupleflow.runtime.FileSource;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

/**
 *
 * @author irmarc
 */
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
public class VocabularySource implements ExNihiloSource<KeyValuePair> {

  Counter vocabCounter;
  Counter skipCounter;
  public Processor<KeyValuePair> processor;
  BTreeReader reader;
  BTreeIterator iterator;
  HashSet<String> inclusions = null;
  HashSet<String> exclusions = null;

  public VocabularySource(TupleFlowParameters parameters) throws Exception {
    String partPath = parameters.getJSON().getString("filename");
    reader = BTreeFactory.getBTreeReader(partPath);
    vocabCounter = parameters.getCounter("terms read");
    skipCounter = parameters.getCounter("terms skipped");
    iterator = reader.getIterator();

    // Look for queries to base the extraction
    Parameters p = parameters.getJSON();
    inclusions = new HashSet<>();
    if (p.isString("includefile")) {
      File f = new File(p.getString("includefile"));
      if (f.exists()) {
        System.err.printf("Opening inclusion file: %s\n", f.getCanonicalPath());
        inclusions = Utility.readFileToStringSet(f);
      }
    } else if (p.isList("include")) {
      List<String> inc = p.getList("include", String.class);
      for (String s : inc) {
        inclusions.add(s);
      }
    }

    exclusions = new HashSet<>();
    if (p.isString("excludefile")) {
      File f = new File(p.getString("excludefile"));
      if (f.exists()) {
        System.err.printf("Opening exclusion file: %s\n", f.getCanonicalPath());
        exclusions = Utility.readFileToStringSet(f);
      }
    } else if (p.isList("exclude")) {
      List<String> inc = p.getList("exclude", String.class);
      for (String s : inc) {
        exclusions.add(s);
      }
    }
  }

  public void run() throws IOException {
    KeyValuePair kvp;
    int number = 0;
    while (!iterator.isDone()) {

      // Filter if we need to
      if (!inclusions.isEmpty() || !exclusions.isEmpty()) {
        String s = ByteUtil.toString(iterator.getKey());
        if (!inclusions.contains(s)) {
          iterator.nextKey();
          skipCounter.increment();
          continue;
        }

        if (exclusions.contains(s)) {
          iterator.nextKey();
          skipCounter.increment();
          continue;
        }
      }

      kvp = new KeyValuePair();
      kvp.key = iterator.getKey();
      kvp.value = Utility.fromInt(number);
      processor.process(kvp);
      vocabCounter.increment();
      number++;
      iterator.nextKey();
    }
    processor.close();
    reader.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    FileSource.verify(parameters, store);
    String partPath = parameters.getJSON().getString("filename");
    try {
      if (!BTreeFactory.isBTree(partPath)) {
        store.addError(partPath + " is not an index file.");
      }
    } catch (FileNotFoundException fnfe) {
      store.addError(partPath + " could not be found.");
    } catch (IOException ioe) {
      store.addError("Generic IO error: " + ioe.getMessage());
    }
  }
}
