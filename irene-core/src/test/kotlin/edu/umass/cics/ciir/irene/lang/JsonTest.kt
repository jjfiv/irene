package edu.umass.cics.ciir.irene.lang

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.Assert.assertEquals
import org.junit.Test

val mapper = ObjectMapper().registerKotlinModule().registerModule(QExprModule())

/**
 *
 * @author jfoley.
 */
class JsonTest {

    @Test
    fun testSDMRoundTrip() {
        val sdm = SequentialDependenceModel(listOf("a", "b", "c"))
        val asJson = mapper.writeValueAsString(sdm)
        val cloned = mapper.readValue(asJson, QExpr::class.java)
        assertEquals(sdm, cloned)
    }

    @Test
    fun testRM3RoundTrip() {
        val expr = RM3Expr(BM25Model(listOf("a", "b", "c")))
        val asJson = mapper.writeValueAsString(expr)
        val cloned = mapper.readValue(asJson, QExpr::class.java)
        assertEquals(expr, cloned)
    }
}