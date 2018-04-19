package edu.umass.cics.ciir.irene.utils

import gnu.trove.map.hash.TObjectDoubleHashMap
import java.util.*

/**
 * Based on Galago's implementation of a FixedSizeMinHeap by Sam Huston.
 * Edited to use an ArrayList instead, and to implement some interfaces.
 *
 * @author jfoley
 */
class TopKHeap<T: Any>(internal val maxSize: Int, internal val cmp: Comparator<T>) : AbstractList<T>() {
    val data: ArrayList<T?> = ArrayList(maxSize)
    internal var fillPtr: Int = 0
    var totalSeen: Long = 0
        internal set

    val isFull: Boolean
        get() = fillPtr >= maxSize

    val sorted: List<T>
        get() {
            val data = unsortedList
            Collections.sort(data, Collections.reverseOrder<T>(cmp))
            return data
        }

    val unsortedList: List<T> get() = data.take(fillPtr).mapNotNull { it }

    init {
        for (i in 0 until maxSize) {
            data.add(null)
        }
    }

    override fun clear() {
        this.fillPtr = 0
        for (i in 0 until maxSize) {
            data.add(null)
        }
    }

    fun peek(): T? {
        return if (fillPtr > 0) {
            data[0]
        } else null
    }

    /**
     * Adds an item to the heap IFF the heaps is small OR the min-item is worse than this item.
     * @return true if the item was kept, false if it was discarded.
     */
    fun offer(d: T?): Boolean {
        totalSeen++
        if (fillPtr < maxSize) {
            // If we've not yet filled the heap, push_back.
            data.set(fillPtr, d)
            fillPtr++
            bubbleUp(fillPtr - 1)

            return true
        } else if (cmp.compare(d, data[0]) > 0) {
            // or if smallest item is worse than this document
            data.set(0, d)
            bubbleDown(0)
            return true
        }

        // No change.
        return false
    }

    fun offerAndCheckFull(d: T): Boolean {
        totalSeen++
        if (fillPtr < maxSize) {
            // If we've not yet filled the heap, push_back.
            data[fillPtr] = d
            fillPtr++
            bubbleUp(fillPtr - 1)

            return fillPtr >= maxSize
        } else if (cmp.compare(d, data[0]) > 0) {
            // or if smallest item is worse than this document
            data[0] = d
            bubbleDown(0)
            return true
        }

        // No change.
        return false
    }

    private fun swap(i: Int, j: Int) {
        val p = data[i]
        data[i] = data[j]
        data[j] = p
    }

    private fun bubbleUp(pos: Int) {
        val parent = (pos - 1) / 2
        if (cmp.compare(data[pos], data[parent]) < 0) {
            swap(pos, parent)
            bubbleUp(parent)
        }
    }

    private fun bubbleDown(pos: Int) {
        val child1 = 2 * pos + 1
        val child2 = child1 + 1

        var selectedChild = if (child1 < fillPtr) child1 else -1

        if (child2 < fillPtr) {
            selectedChild = if (cmp.compare(data[child1], data[child2]) < 0) child1 else child2
        }

        // the parent is bigger than the child (assuming a child)
        if (selectedChild > 0 && cmp.compare(data[pos], data[selectedChild]) > 0) {
            swap(selectedChild, pos)
            bubbleDown(selectedChild)
        }
    }

    override fun add(element: T?): Boolean {
        return offer(element)
    }

    override fun addAll(elements: Collection<T>): Boolean {
        val candidates = ArrayList(elements)
        Collections.sort(candidates, cmp)

        // Add to heap until one of them returns false.
        var change = false
        for (candidate in candidates) {
            if (!offer(candidate)) {
                break
            }
            change = true
        }

        return change
    }

    override fun get(index: Int): T { return data[index]!! }
    override val size: Int get() = fillPtr
}

interface WeightedForHeap {
    val weight: Float
}
data class WeightedWord(override val weight: Float, val word: String): WeightedForHeap, Comparable<WeightedWord> {
    override fun compareTo(other: WeightedWord): Int {
        val cmp = weight.compareTo(other.weight)
        if (cmp != 0) return cmp
        return word.compareTo(other.word)
    }
}

fun TObjectDoubleHashMap<String>.take(k: Int): List<WeightedWord> {
    val heap = ScoringHeap<WeightedWord>(k)
    this.forEachEntry { term, weight ->
        val fw = weight.toFloat()
        heap.offer(fw, { WeightedWord(fw, term) })
    }
    return heap.sorted
}

class ScoringHeap<T: WeightedForHeap>(val maxSize: Int): kotlin.collections.AbstractList<T>() {
    val data: ArrayList<T> = ArrayList(maxSize)
    var fillPtr: Int = 0
    var totalSeen: Long = 0
    inline val isFull: Boolean
        get() = fillPtr >= maxSize
    val sorted: List<T> get() = unsortedList.sortedByDescending { it.weight }
    val unsortedList: List<T> get() = data.take(fillPtr)

    fun peek(): T? {
        return if (fillPtr > 0) {
            data[0]
        } else null
    }

    // Return bottom or worst.
    val min: Float get() = peek()?.weight ?: -Float.MAX_VALUE

    /**
     * Adds an item to the heap IFF the heaps is small OR the min-item is worse than this item.
     * This saves on allocation by only creating a result object if it fits in the heap:
     * @return if kept.
     */
    inline fun offer(score: Float, createIfNeeded: ()->T): Boolean {
        totalSeen++
        if (fillPtr < maxSize) {
            // If we've not yet filled the heap, push_back.
            data.add(createIfNeeded())
            fillPtr++
            bubbleUp(fillPtr - 1)
            return true
        } else if (score > data[0].weight) {
            // or if smallest item is worse than this document
            data[0] = createIfNeeded()
            bubbleDown(0)
            return true
        }
        return false
    }
    fun offer(item: T) = offer(item.weight, {item})

    private fun swap(i: Int, j: Int) {
        val p = data[i]
        data[i] = data[j]
        data[j] = p
    }

    fun bubbleUp(pos: Int) {
        val parent = (pos - 1) / 2
        if (data[pos].weight < data[parent].weight) {
            swap(pos, parent)
            bubbleUp(parent)
        }
    }

    fun bubbleDown(pos: Int) {
        val child1 = 2 * pos + 1
        val child2 = child1 + 1

        var selectedChild = if (child1 < fillPtr) child1 else -1

        if (child2 < fillPtr) {
            selectedChild = if (data[child1].weight < data[child2].weight) child1 else child2
        }

        // the parent is bigger than the child (assuming a child)
        if (selectedChild > 0 && data[pos].weight > data[selectedChild].weight) {
            swap(selectedChild, pos)
            bubbleDown(selectedChild)
        }
    }

    override fun get(index: Int): T { return data[index] }
    override val size: Int get() = fillPtr
}
