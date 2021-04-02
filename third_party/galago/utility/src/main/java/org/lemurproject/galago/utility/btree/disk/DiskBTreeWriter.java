package org.lemurproject.galago.utility.btree.disk;

import org.lemurproject.galago.utility.*;
import org.lemurproject.galago.utility.btree.BTreeWriter;
import org.lemurproject.galago.utility.btree.IndexElement;
import org.lemurproject.galago.utility.compression.VByte;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.utility.debug.NullCounter;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author jfoley.
 */
public class DiskBTreeWriter implements BTreeWriter {
  private DataOutputStream output;
  private VocabularyWriter vocabulary;
  private Parameters manifest;
  private ArrayList<IndexElement> lists;
  private int blockSize;
  private int maxKeySize;
  private int keyOverlap;
  private long filePosition = 0;
  private long listBytes = 0;
  private long keyCount = 0;
  private int blockCount = 0;
  private byte[] prevKey = ByteUtil.EmptyArr;
  private byte[] lastKey = ByteUtil.EmptyArr;
  Counter recordsWritten = NullCounter.instance;
  Counter blocksWritten = NullCounter.instance;

  /**
   * Creates a new DiskBTreeWriter for a file.
   */
  public DiskBTreeWriter(String outputFilename, Parameters parameters) throws IOException {
    this(StreamCreator.openOutputStream(outputFilename), parameters);
  }

  /**
   * Creates a new DiskBTreeWriter with an output stream.
   */
  public DiskBTreeWriter(DataOutputStream output, Parameters parameters) throws IOException {
    this.output = output;
    // max sizes - each defaults to a max length of 2 bytes (short)
    blockSize = parameters.get("blockSize", 16383);
    maxKeySize = parameters.get("keySize", Math.min(blockSize, 16383));
    keyOverlap = maxKeySize;

    vocabulary = new VocabularyWriter();
    manifest = Parameters.create();
    manifest.copyFrom(parameters);
    lists = new ArrayList<>();

    manifest.set("blockSize", blockSize);
    manifest.set("maxKeySize", maxKeySize);
    //manifest.set("maxKeyOverlap", keyOverlap);
  }

  public DiskBTreeWriter(String outputFilename) throws IOException {
    this(outputFilename, Parameters.create());
  }

  /**
   * Returns the current copy of the manifest, which will be stored in
   * the completed index file.  This data is not written until close() is called.
   */
  @Override
  public Parameters getManifest() {
    return manifest;
  }

  /**
   * Keys must be in ascending order
   */
  @Override
  public void add(IndexElement list) throws IOException {
    if (prevKey.length > 0 && CmpUtil.compare(prevKey, list.key()) > 0) {
      throw new IOException(String.format("Key %s, %s are out of order.", ByteUtil.toString(prevKey), ByteUtil.toString(list.key())));
    }
    if (list.key().length >= this.maxKeySize || list.key().length >= blockSize) {
      throw new IOException(String.format("Key %s is too long.", ByteUtil.toString(list.key())));
    }

    if (needsFlush(list)) {
      flush();
    }
    lists.add(list);
    updateBufferedSize(list);
    recordsWritten.increment();

    keyCount++;
    prevKey = list.key();
  }

  @Override
  public void close() throws IOException {
    flush();

    // increment the final key (this writes the first key that is outside the index.
    lastKey = increment(lastKey);
    assert (lastKey.length < Integer.MAX_VALUE) : "Final key issue - can not be written.";

    byte[] vocabularyData = vocabulary.data();
    manifest.set("emptyIndexFile", (vocabularyData.length == 0));
    manifest.set("keyCount", this.keyCount);
    manifest.set("blockCount", this.blockCount);

    byte[] xmlData = manifest.toString().getBytes("UTF-8");
    long vocabularyOffset = filePosition;
    long manifestOffset = filePosition
      + 4 + lastKey.length // final key - part of vocab
      + vocabularyData.length;

    // need to write an int here - key could be very large.
    //  - we are using compression in other places
    output.writeInt(lastKey.length);
    output.write(lastKey);

    output.write(vocabularyData);
    output.write(xmlData);

    output.writeLong(vocabularyOffset);
    output.writeLong(manifestOffset);
    output.writeInt(blockSize);
    output.writeLong(DiskBTreeFormat.MAGIC_NUMBER);

    output.close();
  }

  // Private functions
  private void writeBlock(List<IndexElement> blockLists, long length) throws IOException {
    assert length <= blockSize || blockLists.size() == 1;
    assert wordsInOrder(blockLists);

    if (blockLists.isEmpty()) {
      return;
    }

    // -- compute the length of the block --
    ListData listData = new ListData(blockLists);

    // create header data
    byte[] headerBytes = getBlockHeader(blockLists);

    long startPosition = filePosition;
    long endPosition = filePosition + headerBytes.length + listData.encodedLength();
    assert endPosition <= startPosition + length;
    assert endPosition > startPosition;
    assert filePosition >= Integer.MAX_VALUE || filePosition == output.size();

    // -- begin writing the block --
    vocabulary.add(blockLists.get(0).key(), startPosition, headerBytes.length);

    // key data
    output.write(headerBytes);

    // write inverted list binary data
    listData.write(output);

    filePosition = endPosition;
    assert filePosition >= Integer.MAX_VALUE || filePosition == output.size();
    assert endPosition - startPosition <= blockSize || blockLists.size() == 1;

    blockCount++;
    blocksWritten.increment();
  }

  /**
   * Gives a conservative estimate of the buffered size of the data,
   * excluding the most recent inverted list.
   * Does not include savings due to key overlap compression.
   */
  private long bufferedSize() {
    return listBytes + 8; // key count;
  }

  private void updateBufferedSize(IndexElement list) {
    listBytes += invertedListLength(list);
  }

  private long invertedListLength(IndexElement list) {
    long listLength = 0;

    listLength += list.key().length;
    listLength += 2; // key overlap
    listLength += 2; // key length
    listLength += 2; // file offset bytes

    listLength += list.dataLength();
    return listLength;
  }

  /**
   * Flush all lists out to disk.
   */
  private void flush() throws IOException {
    // if there aren't any lists, quit now
    if (lists.isEmpty()) {
      return;
    }
    writeBlock(lists, bufferedSize());

    // remove all of the current data
    lists = new ArrayList<>();
    listBytes = 0;
  }

  private boolean needsFlush(IndexElement list) {
    long listExtra =
      2 + // byte for key length
        2;  // byte for overlap with previous key

    long bufferedBytes = bufferedSize()
      + invertedListLength(list)
      + listExtra;

    return bufferedBytes >= blockSize;
  }

  /**
   * sjh: function generates a key greater than the input key
   *      - this is necessary to ensure the 'final key
   *      - it may increase the length of the key
   */
  private static byte[] increment(byte[] key) {
    byte[] newData = Arrays.copyOf(key, key.length);
    int i = newData.length - 1;
    while (i >= 0 && newData[i] == Byte.MAX_VALUE) {
      i--;
    }
    if (i >= 0) {
      newData[i]++;
    } else {
      newData = Arrays.copyOf(key, key.length + 1);
    }
    assert (CmpUtil.compare(key, newData) < 0);
    return newData;
  }

  /**
   * Returns true if the lists are sorted in ascending order by
   * key.
   */
  private static boolean wordsInOrder(List<IndexElement> blockLists) {
    for (int i = 0; i < blockLists.size() - 1; i++) {
      boolean result = CmpUtil.compare(blockLists.get(i).key(), blockLists.get(i + 1).key()) <= 0;
      if (!result) {
        return false;
      }
    }
    return true;
  }

  private int prefixOverlap(byte[] firstTerm, byte[] lastTerm) {
    int maximum = Math.min(firstTerm.length, lastTerm.length);
    maximum = Math.min(this.keyOverlap - 1, maximum);

    for (int i = 0; i < maximum; i++) {
      if (firstTerm[i] != lastTerm[i]) {
        return i;
      }
    }
    return maximum;
  }

  private byte[] getBlockHeader(List<IndexElement> blockLists) throws IOException {
    ListData listData;
    ArrayList<byte[]> keys;
    ByteArrayOutputStream wordByteStream = new ByteArrayOutputStream();
    DataOutputStream vocabOutput = new DataOutputStream(wordByteStream);

    listData = new ListData(blockLists);
    keys = new ArrayList<>();
    for (IndexElement list : blockLists) {
      keys.add(list.key());
    }

    vocabOutput.writeLong(keys.size());
    long totalListData = listData.length();
    long invertedListBytes = 0;

    byte[] word = listData.blockLists.get(0).key();
    byte[] lastWord = word;
    assert word.length < this.maxKeySize;

    // this is the first word in the block
    VByte.compressInt(vocabOutput, word.length);
    vocabOutput.write(word, 0, word.length);

    invertedListBytes += listData.blockLists.get(0).dataLength();
    assert totalListData - invertedListBytes < this.blockSize;
    assert totalListData >= invertedListBytes;
    VByte.compressInt(vocabOutput, (int) (totalListData - invertedListBytes));

    for (int j = 1; j < keys.size(); j++) {
      assert word.length < this.maxKeySize;
      word = listData.blockLists.get(j).key();
      int common = this.prefixOverlap(lastWord, word);
      VByte.compressInt(vocabOutput, common);
      VByte.compressInt(vocabOutput, word.length);
      vocabOutput.write(word, common, word.length - common);
      invertedListBytes += listData.blockLists.get(j).dataLength();
      assert totalListData - invertedListBytes < this.blockSize;
      assert totalListData >= invertedListBytes;
      VByte.compressInt(vocabOutput, (int) (totalListData - invertedListBytes));
      lastWord = word;
    }
    vocabOutput.close();

    return wordByteStream.toByteArray();
  }

  // private class to hold a list of index elements (key-value_ pairs)
  private static class ListData {

    List<IndexElement> blockLists;

    public ListData(List<IndexElement> blockLists) {
      this.blockLists = blockLists;
    }

    public long length() {
      long totalLength = 0;
      for (IndexElement e : blockLists) {
        totalLength += e.dataLength();
      }
      return totalLength;
    }

    public long encodedLength() {
      return length();
    }

    public void write(OutputStream stream) throws IOException {
      for (IndexElement e : blockLists) {
        e.write(stream);
      }
    }
  }
}
