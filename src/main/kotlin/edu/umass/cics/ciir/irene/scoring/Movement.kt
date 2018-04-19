package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.lang.*
import org.apache.lucene.index.Term
import org.apache.lucene.search.DocIdSetIterator

fun createMover(q: QExpr, ctx: IQContext): QueryMover = createMoverRec(q, ctx)

private fun createMoverRec(q: QExpr, ctx: IQContext) : QueryMover = when(q) {
    // OR nodes:
    is SynonymExpr, is OrExpr, is CombineExpr, is MultExpr, is MaxExpr, is MultiExpr -> createOrMover(q.children.map { createMoverRec(it, ctx) })

    // Leaves:
    is TextExpr -> ctx.termMover(Term(q.countsField(), q.text))
    is LuceneExpr -> TODO("LuceneExpr")

    // Never score these subtrees.
    NeverMatchLeaf -> NeverMatchMover("literal")
    // Never score a document just because of a constant or prior.
    is ConstBoolExpr, is ConstScoreExpr, is ConstCountExpr -> NeverMatchMover(q.toString())

    // Always match these subtrees rather than create sophisticated children.
    AlwaysMatchLeaf, is LengthsExpr -> AlwaysMatchMover(q.toString(), ctx.numDocs())

// AND nodes:
    is ProxExpr, is UnorderedWindowCeilingExpr, is SmallerCountExpr,
    is AndExpr, is OrderedWindowExpr, is UnorderedWindowExpr -> createAndMover(q.children.map { createMoverRec(it, ctx) }, ctx.numDocs())

// Transformers are straight-forward:
    is CountToScoreExpr, is BoolToScoreExpr, is CountToBoolExpr, is AbsoluteDiscountingQLExpr, is BM25Expr, is WeightExpr, is DirQLExpr -> createMoverRec(q.trySingleChild, ctx)

// NOTE: Galago semantics, only look at cond. This is not an AND like you might think.
    is RequireExpr -> createMoverRec(q.cond, ctx)
    is WhitelistMatchExpr -> {
        val matching = ctx.selectRelativeDocIds(q.docIdentifiers!!)
        if (matching.isEmpty()) {
            NeverMatchMover("Whitelist.Empty")
        } else {
            IntSetMover(matching)
        }
    }
    // Ditch the equals part for mover... this is probably really slow...
    is CountEqualsExpr -> createMoverRec(q.child, ctx)
}

/** Borrow this constant locally. */
const val MOVER_DONE = DocIdSetIterator.NO_MORE_DOCS
interface QueryMover {
    fun docID(): Int
    fun matches(doc: Int): Boolean
    // Used to accelerate AND and OR matching if accurate.
    fun estimateDF(): Long
    // Move forward to (docID() >= target) don't bother checking for match.
    fun advance(target: Int): Int

    // The following movements should never be overridden.
    // They may be used as convenient to express other operators, i.e. call nextMatching on your children instead of advance().

    // Use advance to move forward until match.
    fun nextMatching(doc: Int): Int {
        var id = maxOf(doc, docID());
        while(id < MOVER_DONE) {
            if (matches(id)) {
                assert(docID() >= id)
                return id
            }
            id = advance(id+1)
        }
        return id;
    }
    // Step to the next matching document.
    fun nextDoc(): Int = nextMatching(docID()+1)
    // True if iterator is exhausted.
    val done: Boolean get() = docID() == MOVER_DONE
    // Safe interface to advance. Only moves forward if need be.
    fun syncTo(target: Int) {
        if (docID() < target) {
            advance(target)
            assert(docID() >= target)
        }
    }

    fun collectList(): List<Int> {
        val output = ArrayList<Int>(this.estimateDF().toInt())
        while(!this.done) {
            output.add(docID())
            nextDoc()
        }
        return output
    }
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class AndMover(val children: List<QueryMover>) : QueryMover {
    val N = children.size
    private var current: Int = 0
    val cost = children.map { it.estimateDF() }.min() ?: 0L
    val moveChildren = children.sortedBy { it.estimateDF() }
    init {
        advanceToMatch()
    }
    override fun docID(): Int = current
    fun advanceToMatch(): Int {
        while(current < NO_MORE_DOCS) {
            var match = true
            for (child in moveChildren) {
                var pos = child.docID()
                if (pos < current) {
                    pos = child.nextMatching(current)
                }
                if (pos > current) {
                    current = pos
                    match = false
                    break
                }
            }

            if (match) return current
        }
        return current
    }

    override fun advance(target: Int): Int {
        if (current < target) {
            current = target
            return advanceToMatch()
        }
        return current
    }
    override fun estimateDF(): Long = cost

    override fun matches(doc: Int): Boolean {
        if (current > doc) return false
        if (current == doc) return true
        return advance(doc) == doc
    }
}

/**
 * Create an [OrMover] if need be, optimizing based on actual understanding of children.
 * If any child is an [AlwaysMatchMover], return that instead.
 * Skip any children that are [NeverMatchMover]s.
 * If there's only one child left, return that. Otherwise, construct an [OrMover] of the rest.
 */
fun createOrMover(children: List<QueryMover>): QueryMover {
    val keep = ArrayList<QueryMover>(children.size)
    for (c in children) {
        if (c is AlwaysMatchMover) {
            return c
        }
        if (c is NeverMatchMover) {
            continue
        }
        keep.add(c)
    }
    if (keep.isEmpty()) {
        return NeverMatchMover("Empty-Or")
    }
    if (keep.size == 1) {
        return keep[0]
    }
    return OrMover(keep)
}

/**
 * Create an [AndMover] if need be, optimizing based on actual understanding of children.
 * If any child is an [NeverMatchMover], return that instead.
 * Skip any children that are [AlwaysMatchMover]s.
 * If there's only one child left, return that. Otherwise, construct an [AndMover] of the rest.
 */
fun createAndMover(children: List<QueryMover>, numDocs: Int): QueryMover {
    val keep = ArrayList<QueryMover>(children.size)
    for (c in children) {
        if (c is AlwaysMatchMover) {
            continue
        }
        if (c is NeverMatchMover) {
            return c
        }
        keep.add(c)
    }
    if (keep.isEmpty()) {
        return AlwaysMatchMover("Empty-And", numDocs)
    }
    if (keep.size == 1) {
        return keep[0]
    }
    return AndMover(keep)
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class OrMover(val children: List<QueryMover>) : QueryMover {
    var current = children.map { it.nextMatching(0) }.min()!!
    val cost = children.map { it.estimateDF() }.max() ?: 0L
    val moveChildren = children.sortedByDescending { it.estimateDF() }
    override fun docID(): Int = current
    override fun advance(target: Int): Int {
        var newMin = NO_MORE_DOCS
        for (child in moveChildren) {
            var where = child.docID()
            if (where < target) {
                where = child.advance(target)
            }
            assert(where >= target)
            newMin = minOf(newMin, where)
        }
        current = newMin
        return current
    }
    override fun estimateDF(): Long = cost

    override fun matches(doc: Int): Boolean {
        syncTo(doc)
        assert(docID() >= doc)
        return children.any { it.matches(doc) }
    }
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class NeverMatchMover(val name: String) : QueryMover {
    override fun docID(): Int = MOVER_DONE
    override fun matches(doc: Int): Boolean = false
    override fun estimateDF(): Long = 0
    override fun advance(target: Int): Int = MOVER_DONE
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class AlwaysMatchMover(val name: String, val numDocs: Int): QueryMover {
    var current = 0
    override fun docID(): Int = current
    override fun matches(doc: Int): Boolean {
        advance(doc)
        return (current == doc)
    }
    override fun estimateDF(): Long = numDocs.toLong()
    override fun advance(target: Int): Int {
        current = maxOf(target, current)
        if (current >= numDocs) {
            current = NO_MORE_DOCS
        }
        return current
    }
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class LuceneDocsMover(val name: String, val iter: DocIdSetIterator) : QueryMover {
    override fun docID(): Int = iter.docID()
    override fun matches(doc: Int): Boolean {
        syncTo(doc)
        return docID() == doc
    }
    override fun estimateDF(): Long = iter.cost()
    override fun advance(target: Int): Int {
        // Advance in lucene is not safe if past the value, so don't just proxy.
        if (iter.docID() < target) {
            return iter.advance(target)
        }
        return iter.docID()
    }

}

class IntSetMover(xs: Collection<Int>): QueryMover {
    val ordered = (xs).sorted()
    var pos = 0
    override val done: Boolean get() = pos >= ordered.size
    override fun docID(): Int = if (done) NO_MORE_DOCS else ordered[pos]
    override fun matches(doc: Int): Boolean {
        syncTo(doc)
        if (done) return false
        return ordered[pos] == doc
    }
    override fun estimateDF(): Long = ordered.size.toLong()
    override fun advance(target: Int): Int {
        if (done) return NO_MORE_DOCS
        while(!done && ordered[pos] < target) {
            pos++
        }
        return docID()
    }
}

