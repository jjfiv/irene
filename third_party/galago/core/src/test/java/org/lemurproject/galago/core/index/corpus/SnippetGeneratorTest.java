// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.index.corpus;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class SnippetGeneratorTest {
  @Test
  public void testSimpleSnippet() throws IOException {
    SnippetGenerator generator = new SnippetGenerator();
    List<String> terms = Arrays.asList(new String[] { "some", "text" });
    HashSet<String> query = new HashSet<String>(terms);
    String result = generator.getSnippet("This is some document text", query);
    assertEquals("This is <strong>some</strong> document <strong>text</strong>", result);
  }

  public void testNoDocumentText() throws IOException {
    SnippetGenerator generator = new SnippetGenerator();
    List<String> terms = Arrays.asList(new String[] { "some", "text" });
    HashSet<String> query = new HashSet<String>(terms);
    String result = generator.getSnippet("", query);
    assertEquals("", result);
  }
}
