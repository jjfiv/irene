package edu.umass.cics.ciir.irene.utils

import java.util.*

/**
 * @author jfoley.
 */

fun Int.nearestPowerOfTwo(): Int {
    if(this < 0) throw IllegalArgumentException();
    if(this == 0) return 1

    val nz = Integer.numberOfLeadingZeros(this);
    val numberOfOnes = 32 - nz;
    if(numberOfOnes > 31) throw RuntimeException();
    return 1 shl numberOfOnes;
}

class IntList(var data: IntArray, var fill: Int = data.size) : MutableList<Int>, java.util.AbstractList<Int>() {
    override val size: Int get() = fill
    constructor() : this(IntArray(16), 0)
	constructor(size: Int) : this(IntArray(size), 0)
	constructor(input: Collection<Int>): this(input.toIntArray(), input.size)

    override fun addAll(elements: Collection<Int>): Boolean {
		reserve(elements.size + fill)
		elements.forEach {
			data[fill++] = it
		}
		return elements.size > 0
    }
    override operator fun get(index: Int): Int = data[index]

//
//	public boolean pushAll(IntList right) {
//		final int size = right.size();
//		reserve(fill + size);
//		final int[] rhs = right.unsafeArray();
//		System.arraycopy(rhs, 0, data, fill, size);
//		fill += size;
//		return true;
//	}
//
	/**
	 * Resize to exact value.
	 * @param amt the exact number of items this should be able to hold.
	 */
	fun resize(amt: Int) {
		data = Arrays.copyOf(data, amt)
        fill = Math.min(fill, amt)
    }

	/**
	 * Reserve at least amt number of items; uses the nearest power of two
	 * @param amt number of items to reserve
	 */
	fun reserve(amt: Int) {
		if (amt >= data.size) {
			val size = amt.nearestPowerOfTwo()
            assert(size >= amt)
            data = Arrays.copyOf(data, size)
        }
    }
    fun push(x: Int) {
        reserve(fill+1)
        data[fill++] = x
    }
    override fun add(element: Int): Boolean {
        push(element)
        return true
    }
	override fun clear() {
        fill = 0
    }
	override operator fun set(index: Int, element: Int): Int {
		val prev = data[index]
		data[index] = element
		return prev
    }

    override fun equals(other: Any?): Boolean {
		if (other == this) return true
		if (other is IntList) {
			if (fill != other.fill) return false
			(0 until fill).forEach { i ->
				if (data[i] != other.data[i]) return false
			}
			return true
        }
        return super<AbstractList>.equals(other)
    }
	override fun hashCode(): Int {
		var code = 1
		(0 until fill).forEach { i ->
			code = 31 * code + data[i].hashCode()
		}
		return code
    }

	fun copyToArray(): IntArray = Arrays.copyOfRange(data, 0, fill)
	fun unsafeArray(): IntArray = data
	@Suppress("NOTHING_TO_INLINE")
    inline fun getQuick(key: Int): Int = data[key]

	fun sort() {
		Arrays.sort(data, 0, fill)
    }
	fun unsafeExpandExact(amount: Int) {
        fill = amount
        if (fill > data.size) {
            resize(fill)
        }
    }
	fun swap(i: Int, j: Int) {
        val tmp = data[i]
        data[i] = data[j]
        data[j] = tmp
    }
	val last: Int get() = data[fill-1]

	inline fun forEach(fn: (Int)->Unit) {
		(0 until fill).forEach {i -> fn(data[i]) }
	}

    fun search(doc: Int): Boolean {
		if (fill < 32) {
			forEach { if (it == doc) return true }
			return false
		}
		return Arrays.binarySearch(data, 0, fill, doc) >= 0
	}

}
