package edu.umass.cics.ciir.irene.indexing

import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.galago.getStr
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test

/**
 * @author jfoley
 */
class IteratorTokenStreamTest {
    @Test
    fun testListTokenizer() {
        val rawText = "abc|def|ghi|jkl"
        val text = "abc|def|ghi|jkl".split('|')

        IreneIndexer.build {
            inMemory()
        }.use { indexer ->
            indexer.doc {
                setId("testDoc")
                setTextField(indexer.params.defaultField, rawText, text, true)
            }
            indexer.commit()
            indexer.open().use { index ->
                val internal = index.documentById("testDoc") ?: return fail("Document not indexed.")
                val fields = index.docAsParameters(internal) ?: return fail("Document not stored.")

                assertEquals("testDoc", fields.getStr("id"))
                assertEquals(rawText, fields.getStr("body"))
                val cstats = index.getStats("abc")

                assertEquals(1, cstats.df)
            }
        }

    }
}

