package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.IreneIndexer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.junit.Assert
import org.junit.Test

/**
 * @author jfoley
 */
class RetrievalModelsKtTest {
    @Test
    fun testExactMatchingStrings() {
        val data = listOf("a b c d", "a b c", "a b", "x a b c")
        IreneIndexer.build {
            inMemory()
            defaultAnalyzer = WhitespaceAnalyzer()
        }.use { writer ->
            data.forEachIndexed { i, txt ->
                writer.doc {
                    setId(i.toString())
                    setTextField(writer.params.defaultField, txt)
                }
            }

            writer.commit()
            writer.open()
        }.use { reader ->
            val q = generateExactMatchQuery(listOf("a", "b", "c"))
            //(0 until 4).forEach { docid ->
                //inner = reader.documentById(docid.toString()) ?: error("No document index at $docid")
                //reader.explain(q.deepCopy(), inner))
            //}

            val results = reader.search(q, 100)
            Assert.assertEquals(1L, results.totalHits)
            Assert.assertEquals(1, results.scoreDocs.size)
            Assert.assertEquals(1, results.scoreDocs[0].doc)

        }


    }
}