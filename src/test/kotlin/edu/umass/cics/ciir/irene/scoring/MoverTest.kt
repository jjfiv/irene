package edu.umass.cics.ciir.irene.scoring

import org.junit.Assert.assertEquals
import org.junit.Test


/**
 * @author jfoley.
 */
class MoverTest {
    @Test
    fun testAndMovement() {
        val lhs = IntSetMover(listOf(1,2,7,8))
        val rhs = IntSetMover(listOf(1,2,8))

        val and = AndMover(listOf(lhs, rhs))
        assertEquals(listOf(1, 2,8), and.collectList())
    }

    @Test
    fun testAndMovementEmpty() {
        val lhs = IntSetMover(listOf(10,12,17,18))
        val rhs = IntSetMover(listOf(1,2,8))

        val and = AndMover(listOf(lhs, rhs))
        assertEquals(emptyList<Int>(), and.collectList())
    }

    @Test
    fun testOrMovement() {
        val lhs = IntSetMover(listOf(1,2,7,8))
        val rhs = IntSetMover(listOf(1,2,8))

        val and = OrMover(listOf(lhs, rhs))
        assertEquals(listOf(1, 2, 7, 8), and.collectList())
    }

    @Test
    fun testOrMovementDisjoint() {
        val lhs = IntSetMover(listOf(10,12,17,18))
        val rhs = IntSetMover(listOf(1,2,8))

        val and = OrMover(listOf(lhs, rhs))
        assertEquals(listOf(1,2,8,10,12,17,18), and.collectList())
    }
}