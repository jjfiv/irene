// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.btree.format.BTreeFactory;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Reads a binary file of document names produced by DiskNameReverseWriter
 * 
 * @author sjh
 */
public class DiskNameReverseReader extends KeyValueReader implements NamesReverseReader {

  public DiskNameReverseReader(String fileName) throws IOException {
    super(BTreeFactory.getBTreeReader(fileName));
  }

  public DiskNameReverseReader(BTreeReader r) {
    super(r);
  }

  // gets the document id for some document name
  @Override
  public long getDocumentIdentifier(String documentName) throws IOException {
    byte[] data = reader.getValueBytes(ByteUtil.fromString(documentName));
    if (data == null) {
      return -1;
    }
    return Utility.toLong(data);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    return Collections.emptyMap();
  }

  @Override
  public BaseIterator getIterator(Node node) throws IOException {
    throw new UnsupportedOperationException("Index doesn't support operator: " + node.getOperator());
  }

  public static class KeyIterator extends KeyValueReader.KeyValueIterator {

    protected BTreeReader input;

    public KeyIterator(BTreeReader input) throws IOException {
      super(input);
    }

    public boolean skipToKey(String name) throws IOException {
      return findKey(ByteUtil.fromString(name));
    }

    public String getCurrentName() throws IOException {
      return ByteUtil.toString(getKey());
    }

    public long getCurrentIdentifier() throws IOException {
      return Utility.toLong(getValueBytes());
    }

    @Override
    public String getValueString() {
      try {
        return Long.toString(Utility.toLong(getValueBytes()));
      } catch (IOException e) {
        return "Unknown";
      }
    }

    @Override
    public String getKeyString() {
      return ByteUtil.toString(getKey());
    }

    @Override
    public KeyToListIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
    }
  }
}
