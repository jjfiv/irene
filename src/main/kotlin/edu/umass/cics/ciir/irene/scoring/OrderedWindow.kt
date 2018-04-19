package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.lang.ProxExpr
import edu.umass.cics.ciir.irene.lang.SmallerCountExpr
import edu.umass.cics.ciir.irene.lang.UnorderedWindowCeilingExpr
import edu.umass.cics.ciir.irene.lang.UnorderedWindowExpr
import org.apache.lucene.search.Explanation

/**
 *
 * @author jfoley.
 */
fun countOrderedWindows(arrayIterators: List<PositionsIter>, step: Int): Int {
    var hits = 0
    var notDone = true
    while (notDone) {
        // find the start of the first word
        var invalid = false

        // loop over all the rest of the words
        for (i in 1 until arrayIterators.size) {
            val end = arrayIterators[i - 1].position + 1

            // try to move this iterator so that it's past the end of the previous word
            assert(!arrayIterators[i].done)
            while (end > arrayIterators[i].position) {
                notDone = arrayIterators[i].next()

                // if there are no more occurrences of this word,
                // no more ordered windows are possible
                if (!notDone) {
                    return hits
                }
            }

            if (arrayIterators[i].position - end >= step) {
                invalid = true
                break
            }
        }

        // if it's a match, record it
        if (!invalid) {
            hits++
        }

        // move the first iterator forward - we are double dipping on all other iterators.
        notDone = arrayIterators[0].next()
    }

    return hits
}

fun countUnorderedWindows(iters: List<PositionsIter>, width: Int): Int {
    var hits = 0

    var max = iters[0].position + 1
    var min = iters[0].position
    for (i in 1 until iters.size) {
        val pos = iters[i].position
        max = Math.max(max, pos + 1)
        min = Math.min(min, pos)
    }

    while (true) {
        val match = (max - min <= width) || width == -1
        if (match) {
            //println("$min -> $max")
            hits++
        }

        val oldMin = min
        // now, reset bounds
        max = Integer.MIN_VALUE
        min = Integer.MAX_VALUE
        for (iter in iters) {
            var pos = iter.position
            if (pos == oldMin) {
                val notDone = iter.next()
                if (!notDone) {
                    return hits
                }
                assert(iter.position > oldMin)
            }
            pos = iter.position
            max = maxOf(max, pos + 1)
            min = minOf(min, pos)
        }
    }
}

/** Prox [ProxExpr] differs from [UnorderedWindowExpr] in that we want to compute something that has a stronger upper-bound. */
fun countProxWindows(iters: List<PositionsIter>, width: Int): Int {
    val smallest = iters.minBy { it.size } ?: return 0
    val rest = iters.filter { it !== smallest }

    var hits = 0
    smallest.forAll { candidate ->
        val min = candidate - width
        val max = candidate + width

        var match = true
        for (r in rest) {
            if (!r.advance(min)) return hits
            if (r.position > max) {
                match = false
                break
            }
        }
        if (match) {
            hits++
        }
    }
    return hits
}

class PositionsIter(val data: IntArray, val size: Int=data.size, var index: Int = 0) {
    val done: Boolean get() = index >= size
    fun reset() { index = 0}
    fun next(): Boolean {
        index++
        return !done
    }
    val position: Int get() = data[index]
    val count: Int get() = size

    inline fun forAll(each: (Int)->Unit) {
        (0 until size).forEach { each(data[it]) }
    }
    /** @return true if found (false if [done]). */
    fun advance(target: Int): Boolean {
        while(index < size && data[index] < target) {
            index++
        }
        return index < size
    }

    override fun toString() = (0 until size).map { data[it] }.toList().toString()
}

abstract class CountWindow(children: List<PositionsEvalNode>) : AndEval<PositionsEvalNode>(children), CountEvalNode {
    init {
        assert(children.size > 1)
    }
    var lastDoc = -1
    var lastCount = 0
    abstract fun compute(iters: List<PositionsIter>): Int
    override fun count(env: ScoringEnv): Int {
        val doc = env.doc
        if (doc == lastDoc) return lastCount

        // otherwise, compute!
        val iters = children.map { child ->
            val count = child.count(env)
            if (count == 0) {
                lastDoc = doc
                lastCount = 0
                return 0
            }
            child.positions(env)
        }

        lastDoc = doc
        lastCount = compute(iters)
        return lastCount
    }
    override fun matches(env: ScoringEnv): Boolean {
        return super.matches(env) && count(env) > 0
    }
    override fun toString(): String {
        return children.joinToString(prefix="${this.javaClass.simpleName}[", postfix = "]", separator = ", ")
    }
}

/**
 * [OrderedWindowExpr]
 */
class OrderedWindow(children: List<PositionsEvalNode>, val step: Int) : CountWindow(children) {
    override fun compute(iters: List<PositionsIter>): Int = countOrderedWindows(iters, step)

    /** TODO is the minimum possible at this document is always zero for a [CountEvalNode]? */
    fun min(env: ScoringEnv): Int = 0
    /** Custom maximization logic for [OrderedWindow]. This is the same as [SmallerCountWindow] */
    fun max(env: ScoringEnv): Int {
        var min = Integer.MAX_VALUE
        for (c in children) {
            val cc = c.count(env)
            if (cc == 0) return 0
            min = minOf(cc, min)
        }
        return min
    }

}

class UnorderedWindow(children: List<PositionsEvalNode>, val width: Int) : CountWindow(children) {
    override fun compute(iters: List<PositionsIter>): Int = countUnorderedWindows(iters, width)
    override fun explain(env: ScoringEnv): Explanation {
        val expls = children.map { it.explain(env) }
        if (matches(env)) {
            return Explanation.match(score(env).toFloat(), "$className[$width].Match", expls)
        }
        return Explanation.noMatch("$className[$width].Miss", expls)
    }
}

/** From [ProxExpr] for computing something like [UnorderedWindow] but more realistically upper-bounded by MinCount(children). */
class ProxWindow(children: List<PositionsEvalNode>, val width: Int): CountWindow(children) {
    override fun compute(iters: List<PositionsIter>): Int = countProxWindows(iters, width)
}

/** From [SmallerCountExpr], for estimating the ceiling of [OrderedWindow] nodes. */
class SmallerCountWindow(children: List<QueryEvalNode>) : AndEval<QueryEvalNode>(children), CountEvalNode {
    init {
        assert(children.size > 1)
    }
    override fun count(env: ScoringEnv): Int {
        if (!matches(env)) return 0
        var min = Integer.MAX_VALUE
        for (c in children) {
            val x = c.count(env)
            if (x == 0) return 0
            if (x < min) min = x
        }
        return min
    }
    override fun toString(): String {
        return children.joinToString(separator = ", ", prefix="sc(", postfix=")")
    }
}

/** From [UnorderedWindowCeilingExpr], for estimating the ceiling of [UnorderedWindow] nodes.
 * Consider the terms (if two) as nodes in a bi-partite set, with counts p and q.
 * Therefore, the maximum possible output is the maximum number of nodes, or p * q.
 */
class UnorderedWindowCeiling(val width: Int, children: List<QueryEvalNode>) : AndEval<QueryEvalNode>(children), CountEvalNode {
    init {
        assert(children.size > 1)
    }
    override fun count(env: ScoringEnv): Int {
        if (!matches(env)) return 0
        var max = 0
        var min = Integer.MAX_VALUE
        for (c in children) {
            val x = c.count(env)
            if (x > max) max = x
            if (x < min) min = x
        }
        return max * minOf(min, width)
    }
}
