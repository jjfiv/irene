// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.execution.StepInformation;
import org.lemurproject.galago.utility.*;

import java.io.*;
import java.net.ServerSocket;
import java.util.*;

/**
 * Lots of static methods here that have broad use.
 *
 * @author trevor
 */
public class Utility {
  // Some constant values
  public static final double log2 = Math.log(2);
  public static final double loge_base2 = Math.log(Math.E) / log2;
  public static final double tinyLogProbScore = Math.log(Math.pow(10, -10));

  /**
   * Builds a simple Sorter step that can be added to a TupleFlow stage.
   *
   * @param sortOrder An order object representing how and what to sort.
   * @return a Step object that can be added to a TupleFlow Stage.
   */
  public static StepInformation getSorter(Order sortOrder) {
    return getSorter(sortOrder, null, CompressionType.VBYTE);
  }

  public static StepInformation getSorter(Order sortOrder, CompressionType c) {
    return getSorter(sortOrder, null, c);
  }

  public static StepInformation getSorter(Order sortOrder, Class<?> reducerClass, CompressionType c) {
    org.lemurproject.galago.utility.Parameters p = org.lemurproject.galago.utility.Parameters.create();
    p.set("class", sortOrder.getOrderedClass().getName());
    p.set("order", Utility.join(sortOrder.getOrderSpec()));
    if (c != null) {
      p.set("compression", c.toString());
//      System.err.println("Setting sorter to :" + c.toString() + " -- " + join(sortOrder.getOrderSpec()));
//    } else {
//      System.err.println("NOT setting sorter to : null -- " + join(sortOrder.getOrderSpec()));
    }

    if (reducerClass != null) {
      try {
        reducerClass.asSubclass(Reducer.class);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException("getSorter called with a reducerClass argument "
                + "which is not actually a reducer: "
                + reducerClass.getName());
      }
      p.set("reducer", reducerClass.getName());
    }
    return new StepInformation(Sorter.class, p);
  }

  /**
   * Finds a free port to listen on. Useful for starting up internal web
   * servers. (copied from chaoticjava.com)
   */
  public static int getFreePort() throws IOException {
    ServerSocket server = new ServerSocket(0);
    int port = server.getLocalPort();
    server.close();
    return port;
  }

  public static boolean isInteger(String s) {
    try {
      Integer.parseInt(s);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static String strip(String source, String suffix) {
    if (source.endsWith(suffix)) {
      return source.substring(0, source.length() - suffix.length());
    }

    return null;
  }

  /**
   * For an array master, returns an array containing the last
   * master.length-index elements.
   */
  public static String[] subarray(String[] master, int index) {
    if (master.length <= index) {
      return new String[0];
    } else {
      String[] sub = new String[master.length - index];
      System.arraycopy(master, index, sub, 0, sub.length);
      return sub;
    }
  }

  /**
   * Returns a string containing all the elements of args, space delimited.
   */
  public static String join(String[] args, String delimiter) {
		return join(Arrays.asList(args), delimiter);
  }

  public static String join(List<String> args, String delimiter) {
    StringBuilder builder = new StringBuilder();

    for (String arg : args) {
      if (builder.length() > 0) {
        builder.append(delimiter);
      }
      builder.append(arg);
    }

    return builder.toString();
  }

  public static String join(String[] args) {
    return join(args, " ");
  }

  public static String join(Object[] args, String delimiter) {
    StringBuilder builder = new StringBuilder();

    for (Object arg : args) {
      if (builder.length() > 0) {
        builder.append(delimiter);
      }
      builder.append(arg.toString());
    }

    return builder.toString();
  }

  public static String join(double[] numbers) {
    return join(numbers, ",");
  }

  public static String join(double[] numbers, String delimiter) {
    StringBuilder builder = new StringBuilder();

    for (double arg : numbers) {
      if (builder.length() > 0) {
        builder.append(delimiter);
      }
      builder.append(arg);
    }

    return builder.toString();
  }

  public static String join(int[] numbers) {
    return join(numbers, ",");
  }

  public static String join(int[] numbers, String delimiter) {
    StringBuilder builder = new StringBuilder();

    for (int arg : numbers) {
      if (builder.length() > 0) {
        builder.append(delimiter);
      }
      builder.append(arg);
    }

    return builder.toString();
  }

  public static String join(short[] numbers) {
    return join(numbers, ",");
  }

  public static String join(short[] numbers, String delimiter) {
    StringBuilder builder = new StringBuilder();

    for (short arg : numbers) {
      if (builder.length() > 0) {
        builder.append(delimiter);
      }
      builder.append(arg);
    }

    return builder.toString();
  }

  /**
   * @deprecated use StreamUtil instead.
   */
  @Deprecated
  public static void copyStringToFile(String s, File file) throws IOException {
    StreamUtil.copyStringToFile(s, file);
  }

  public static BufferedReader utf8Reader(String file) throws IOException {
    return utf8Reader(new File(file));
  }

  public static BufferedReader utf8Reader(File fp) throws IOException {
    try {
      return new BufferedReader(new InputStreamReader(StreamCreator.openInputStream(fp), "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static HashSet<String> readStreamToStringSet(InputStream stream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
    HashSet<String> set = new HashSet<>();
    String line;

    while ((line = reader.readLine()) != null) {
      set.add(line.trim());
    }

    reader.close();
    return set;
  }

  public static HashSet<String> readFileToStringSet(File file) throws IOException {
    BufferedReader reader = utf8Reader(file);
    HashSet<String> set = new HashSet<>();
    String line;

    while ((line = reader.readLine()) != null) {
      set.add(line.trim());
    }

    reader.close();
    return set;
  }

  public static String readFileToString(File file) throws IOException {
    BufferedReader reader = null;
    try {
      reader = utf8Reader(file);
      StringBuilder sb = new StringBuilder();
      String line;

      while ((line = reader.readLine()) != null) {
        sb.append(line).append("\n");
      }

      return sb.toString();
    } finally {
      if(reader != null) reader.close();
    }
  }

  public static short toShort(byte[] key) {
    assert (key.length == 2);
    int value = ((key[0] << 8) + key[1]);
    return ((short) value);
  }

  public static byte[] fromShort(short key) {
    byte[] writeBuffer = new byte[2];
    writeBuffer[0] = (byte) ((key >>> 8) & 0xFF);
    writeBuffer[1] = (byte) key;
    return writeBuffer;
  }

  public static boolean isInt(byte[] key) {
    return key != null && key.length == 4;
  }

  public static int toInt(byte[] key) {
    assert isInt(key);
    return (((key[0] & 255) << 24)
            + ((key[1] & 255) << 16)
            + ((key[2] & 255) << 8)
            + (key[3] & 255));
  }

  public static byte[] fromInt(int key) {
    byte[] converted = new byte[4];
    converted[0] = (byte) ((key >>> 24) & 0xFF);
    converted[1] = (byte) ((key >>> 16) & 0xFF);
    converted[2] = (byte) ((key >>> 8) & 0xFF);
    converted[3] = (byte) (key & 0xFF);
    return converted;
  }

  public static boolean isLong(byte[] value) {
    return (value != null && value.length == 8);
  }

  public static long toLong(byte[] key) {
    assert isLong(key);
    return (((long) key[0] << 56)
            + ((long) (key[1] & 255) << 48)
            + ((long) (key[2] & 255) << 40)
            + ((long) (key[3] & 255) << 32)
            + ((long) (key[4] & 255) << 24)
            + ((key[5] & 255) << 16)
            + ((key[6] & 255) << 8)
            + ((key[7] & 255) << 0));
  }

  public static byte[] fromLong(long key) {
    byte[] writeBuffer = new byte[8];

    writeBuffer[0] = (byte) (key >>> 56);
    writeBuffer[1] = (byte) (key >>> 48);
    writeBuffer[2] = (byte) (key >>> 40);
    writeBuffer[3] = (byte) (key >>> 32);
    writeBuffer[4] = (byte) (key >>> 24);
    writeBuffer[5] = (byte) (key >>> 16);
    writeBuffer[6] = (byte) (key >>> 8);
    writeBuffer[7] = (byte) (key >>> 0);
    return writeBuffer;
  }

  /**
   * Check that we are given a byte array of length 1 to parse as a boolean.
   */
  public static boolean isBoolean(byte[] key) {
    return key != null && key.length == 1;
  }

  public static boolean toBoolean(byte[] key) {
    assert isBoolean(key);
    return (key[0] != 0);
  }

  public static byte[] fromBoolean(boolean key) {
    byte[] out = new byte[1];
    if (key) {
      out[0] = 1;
    } else {
      out[0] = 0;
    }
    return out;
  }

  /**
   * This method checks whether the byte array is of the correct size to be a
   * double.
   *
   * @param value - a byte array
   * @return true if non-null and 8 bytes long
   */
  public static boolean isDouble(byte[] value) {
    return (value != null && isLong(value));
  }

  /*
   * NOTE: doubles should NOT be used as index keys
   *  - rounding errors are likely to cause otherwise identical values not to match
   */
  public static double toDouble(byte[] value) {
    assert isDouble(value);
    long l = Utility.toLong(value);
    return Double.longBitsToDouble(l);
  }

  public static byte[] fromDouble(double value) {
    long l = Double.doubleToRawLongBits(value);
    return Utility.fromLong(l);
  }

}
