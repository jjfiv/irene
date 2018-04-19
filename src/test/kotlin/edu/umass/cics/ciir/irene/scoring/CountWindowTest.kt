package edu.umass.cics.ciir.irene.scoring

import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * @author jfoley
 */
class CountWindowTest {
    @Test
    fun testUnorderedWindow() {
        val t1 = intArrayOf(46, 50, 63, 73, 81, 90, 99, 113, 119, 123, 129, 139)
        val t2 = intArrayOf(91, 100, 114)

        val count = countUnorderedWindows(listOf(PositionsIter(t1), PositionsIter(t2)), 8)
        assertEquals(count, 4)
    }

    @Test
    fun testMaxUnorderedWindow() {
        val t1 = intArrayOf(0,1,2,3,4,5,6, 8,9,10,11,12,13,14,15)
        val t2 = intArrayOf(7)
        assertEquals(8, countUnorderedWindows(listOf(PositionsIter(t1), PositionsIter(t2)), 8))
        assertEquals(4, countUnorderedWindows(listOf(PositionsIter(t1), PositionsIter(t2)), 4))
    }

    @Test
    fun testMaxUnorderedWindowDense() {
        val t1 = intArrayOf(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)
        assertEquals(16, countUnorderedWindows(listOf(PositionsIter(t1), PositionsIter(t1)), 8))
        assertEquals(16, countUnorderedWindows(listOf(PositionsIter(t1), PositionsIter(t1)), 4))
    }

    @Test
    fun testMaxUnorderedWindowAgain() {
        val t1 = intArrayOf(45, 48, 58, 65, 70, 83, 92, 100, 108, 113, 123, 130)
        val t2 = intArrayOf(46, 49, 59, 66, 71, 84, 93, 101, 109, 114)
        println(countUnorderedWindows(listOf(PositionsIter(t1), PositionsIter(t2)), 8))
    }
}