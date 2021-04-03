// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.utility.btree.disk;

import org.lemurproject.galago.utility.compression.VByte;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * [sjh] this class could cause problems for VERY large vocabularies
 *
 * @author trevor
 */
public class VocabularyWriter {

  DataOutputStream output;
  ByteArrayOutputStream buffer;

  public VocabularyWriter() throws IOException {
    buffer = new ByteArrayOutputStream();
    output = new DataOutputStream(new BufferedOutputStream(buffer));
  }

  public void add(byte[] key, long offset, int headerLength) throws IOException {
    VByte.compressInt(output, key.length);
    output.write(key);
    VByte.compressLong(output, offset);
    VByte.compressInt(output, headerLength);
  }

  public byte[] data() throws IOException {
    output.close();
    return buffer.toByteArray();
  }
}
