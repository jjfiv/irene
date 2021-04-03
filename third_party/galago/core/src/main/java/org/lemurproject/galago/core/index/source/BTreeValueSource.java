package org.lemurproject.galago.core.index.source;

import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.btree.BTreeIterator;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 *
 * @author jfoley
 */
public abstract class BTreeValueSource implements DiskSource {
  // OPTIONS
  public static final int HAS_SKIPS = 0x01;
  public static final int HAS_MAXTF = 0x02;
  public static final int HAS_INLINING = 0x04;
  
  final protected BTreeIterator btreeIter;
  final protected String key;
  
  public BTreeValueSource(@Nonnull BTreeIterator it) throws IOException {
    this.key = ByteUtil.toString(it.getKey());
    this.btreeIter = it;
  }

  public BTreeValueSource(BTreeIterator it, String displayKey) throws IOException {
    this.key = displayKey;
    this.btreeIter = it;
  }
  
  @Override
  public boolean hasMatch(long id) {
    return !isDone() && currentCandidate() == id;
  }
  
  @Override
  public String key() {
    return key;
  }
}
