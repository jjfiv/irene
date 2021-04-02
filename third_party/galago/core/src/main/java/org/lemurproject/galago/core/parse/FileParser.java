// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.Parameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

/**
 * Reads data from a single text file of type HTML, XML or txt.
 *
 * @author trevor
 */
class FileParser extends DocumentStreamParser {

  BufferedReader reader;
  String identifier;

  public FileParser(DocumentSplit split, Parameters parameters) throws IOException {
    super(split, parameters);
//          Parameters parameters, String fileName, BufferedReader bufferedReader) {
    this.identifier = getIdentifier(parameters, getFileName(split));
    this.reader = getBufferedReader(split);
  }

  public String getIdentifier(Parameters parameters, String fileName) {
    String idType = parameters.get("identifier", "filename");
    if (idType.equals("filename")) {
      return fileName;
    } else {
      String id = stripExtensions(fileName);
      id = new File(id).getName();
      return id;
    }
  }

  public static String stripExtension(String name, String extension) {
    if (name.endsWith(extension)) {
      name = name.substring(0, name.length() - extension.length());
    }
    return name;
  }

  public static String stripExtensions(String name) {
    name = stripExtension(name, ".bz");
    name = stripExtension(name, ".bz2");
    name = stripExtension(name, ".gz");
    name = stripExtension(name, ".html");
    name = stripExtension(name, ".xml");
    name = stripExtension(name, ".txt");
    return name;
  }

  /**
   * finds the first &lt;title&gt; in the string
   */
  public static String getTitle(String text) {
    try {
      int start = text.indexOf("<title>");
      if (start < 0) {
        return "";
      }
      int end = text.indexOf("</title>", start);
      if (end < 0) {
        return "";
      }
      return new String(text.substring(start + "<title>".length(), end));
    } catch (StringIndexOutOfBoundsException ex) {
      return "";
    }
  }

  @Override
  public Document nextDocument() throws IOException {
    if (reader == null || !reader.ready()) {
      return null;
    }

    StringBuilder builder = new StringBuilder();
    String line;

    while ((line = reader.readLine()) != null) {
      builder.append(line);
      builder.append("\n");
    }

    Document result = new Document();
    result.name = identifier;
    result.text = builder.toString();
    result.metadata.put("title", getTitle(result.text));

    return result;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }
}
