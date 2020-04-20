package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.indexing.IreneIndexer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.search.ScoreDoc
import org.junit.Assert
import org.junit.Test

/**
 * @author jfoley
 */
class RetrievalModelsKtTest {

    @Test
    fun testLuceneQuery() {
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
            val q = LuceneExpr("\"a b c\"")
            //(0 until 4).forEach { docid ->
            //inner = reader.documentById(docid.toString()) ?: error("No document index at $docid")
            //reader.explain(q.deepCopy(), inner))
            //}

            val results = reader.search(q, 100)
            Assert.assertEquals(3L, results.totalHits)
            Assert.assertEquals(3, results.scoreDocs.size)

            val qMiss = LuceneExpr("+z a b")
            val noResults = reader.search(qMiss, 4)
            Assert.assertEquals(emptyList<ScoreDoc>(), noResults.scoreDocs.toList())
        }
    }

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