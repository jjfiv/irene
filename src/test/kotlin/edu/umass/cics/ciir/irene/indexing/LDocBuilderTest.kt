package edu.umass.cics.ciir.irene.indexing

import edu.umass.cics.ciir.irene.utils.forAllPairs
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.tokenize
import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * @author jfoley.
 */
class LDocBuilderTest {
    @Test
    fun setEfficientTextField() {
        val document = "To be or not to be, that is the question."

        val params = IndexParams().apply { inMemory() }
        val terms = params.analyzer.tokenize(params.defaultField, document)

        IreneIndexer(params).use { writer ->
            writer.doc {
                setId("test")
                setTextField(params.defaultField, document)
            }
            writer.commit()
            writer.open()
        }.use { slowIndex ->
            IreneIndexer(params).use { writer2 ->
                writer2.doc {
                    setId("test")
                    setEfficientTextField(params.defaultField, document)
                }
                writer2.commit()
                writer2.open()
            }.use { fastIndex ->
                fastIndex.env.indexedBigrams = true
                terms.toSet().forEach { ut ->
                    val scs = slowIndex.getStats(ut)
                    val fcs = fastIndex.getStats(ut)

                    assertEquals(scs.cl, fcs.cl)
                    assertEquals(scs.dc, fcs.dc)
                    assertEquals(scs.df, fcs.df)
                    assertEquals(scs.cf, fcs.cf)
                }

                terms.forAllPairs { lhs, rhs ->
                    val window1 = OrderedWindowExpr(listOf(TextExpr(lhs), TextExpr(rhs)))
                    val window2 = OrderedWindowExpr(listOf(TextExpr(rhs), TextExpr(lhs)))

                    run {
                        val scs = slowIndex.getStats(window1)
                        val fcs = fastIndex.getStats(window1)

                        assertEquals(scs.cl, fcs.cl)
                        assertEquals(scs.dc, fcs.dc)
                        assertEquals(scs.df, fcs.df)
                        assertEquals(scs.cf, fcs.cf)
                    }
                    run {
                        val scs = slowIndex.getStats(window2)
                        val fcs = fastIndex.getStats(window2)

                        assertEquals(scs.cl, fcs.cl)
                        assertEquals(scs.dc, fcs.dc)
                        assertEquals(scs.df, fcs.df)
                        assertEquals(scs.cf, fcs.cf)
                    }
                    run {
                        val slowDoc = slowIndex.search(RequireExpr(AlwaysMatchLeaf, DirQLExpr(window1)), 1).scoreDocs[0]
                        val fastDoc = fastIndex.search(RequireExpr(AlwaysMatchLeaf, DirQLExpr(window1)), 1).scoreDocs[0]

                        if (Math.abs(slowDoc.score - fastDoc.score) >= 1e-7f) {
                            println(slowIndex.explain(DirQLExpr(window1), slowDoc.doc))
                            println(fastIndex.explain(DirQLExpr(window1), fastDoc.doc))
                        }
                        assertEquals(slowDoc.score, fastDoc.score, 1e-7f)
                    }

                }
            }
        }
    }

}