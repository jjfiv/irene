package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.IreneIndexer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.junit.Assert
import org.junit.Test

class DocValueExprsTest {

    @Test
    fun testLongDocValues() {
        IreneIndexer.build {
            inMemory()
            defaultAnalyzer = WhitespaceAnalyzer()
        }.use { writer ->

            writer.doc {
                setId("A")
                setDenseLongField("score", 1);
            }
            writer.doc {
                setId("B")
                setDenseLongField("score", -1);
            }
            writer.doc {
                setId("C")
                setDenseLongField("score", -200);
            }
            writer.doc {
                setId("D")
            }
            writer.commit()
            writer.open()
        }.use { reader ->
            val q = AlwaysMatchExpr(DenseLongField("score", 3))

//            listOf("A", "B", "C", "D").forEach { docid ->
//              val inner = reader.documentById(docid) ?: error("No document index at $docid")
//              println(reader.explain(q.deepCopy(), inner))
//            }

            val results = reader.search(q, 4)
            Assert.assertEquals(4L, results.totalHits)
            Assert.assertEquals(4, results.scoreDocs.size)

            Assert.assertEquals(results.scoreDocs[0].score, 3.0f, 0.01f)
            Assert.assertEquals(results.scoreDocs[1].score, 1.0f, 0.01f)
            Assert.assertEquals(results.scoreDocs[2].score, -1.0f, 0.01f)
            Assert.assertEquals(results.scoreDocs[3].score, -200.0f, 0.01f)
        }
    }

    @Test
    fun testFloatDocValues() {
        IreneIndexer.build {
            inMemory()
            defaultAnalyzer = WhitespaceAnalyzer()
        }.use { writer ->

            writer.doc {
                setId("A")
                setDenseFloatField("score", 1.0f);
            }
            writer.doc {
                setId("B")
                setDenseFloatField("score", -1.5f);
            }
            writer.doc {
                setId("C")
                setDenseFloatField("score", -200.5f);
            }
            writer.doc {
                setId("D")
            }
            writer.commit()
            writer.open()
        }.use { reader ->
            val q = AlwaysMatchExpr(DenseFloatField("score", 3.0f))

//            listOf("A", "B", "C", "D").forEach { docid ->
//                val inner = reader.documentById(docid) ?: error("No document index at $docid")
//                println(reader.explain(q.deepCopy(), inner))
//            }

            val results = reader.search(q, 4)
            Assert.assertEquals(4L, results.totalHits)
            Assert.assertEquals(4, results.scoreDocs.size)

            Assert.assertEquals(results.scoreDocs[0].score, 3.0f, 0.01f)
            Assert.assertEquals(results.scoreDocs[1].score, 1.0f, 0.01f)
            Assert.assertEquals(results.scoreDocs[2].score, -1.5f, 0.01f)
            Assert.assertEquals(results.scoreDocs[3].score, -200.5f, 0.01f)
        }
    }
}