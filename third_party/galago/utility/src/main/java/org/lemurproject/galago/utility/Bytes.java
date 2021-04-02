// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.utility;

import java.util.Arrays;

/**
 * Boxing a byte[] object to ensure proper operation
 *
 * @author irmarc
 */
public class Bytes implements Comparable<Bytes> {
  byte[] bytes;

  public Bytes(byte[] b) {
    bytes = Arrays.copyOf(b, b.length);
  }

  public boolean equals(Bytes that) {
    return Arrays.equals(this.bytes, that.bytes);
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.bytes);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Bytes other = (Bytes) obj;
    return Arrays.equals(this.bytes, other.bytes);
  }

  @Override
  public int compareTo(Bytes that) {
    return CmpUtil.compare(this.bytes, that.bytes);
  }
}
