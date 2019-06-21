package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.lang.*

/**
 * Create movement expressions from a single query expression.
 * @author jfoley
 */
fun createOptimizedMovementExpr(q: QExpr): QExpr = when(q) {
    // OR nodes:
    is SynonymExpr, is OrExpr, is CombineExpr, is MultExpr, is MaxExpr, is MultiExpr -> OrExpr(q.children.map { createOptimizedMovementExpr(it) })

    // Leaves:
    is WhitelistMatchExpr, is TextExpr, is LuceneExpr, is LengthsExpr, is ConstCountExpr, is ConstBoolExpr, is ConstScoreExpr -> q.deepCopy()
    is LongLTE -> q.deepCopy()

    // Not sure how to approximate this, is kind of like a leaf.
    is CountEqualsExpr -> q.deepCopy()

    // AND nodes:
    is MustExpr,
    is AndExpr, is ProxExpr, is UnorderedWindowCeilingExpr, is SmallerCountExpr, is OrderedWindowExpr, is UnorderedWindowExpr -> AndExpr(q.children.map { createOptimizedMovementExpr(it) })

    // Transformers are just pass-through movement.
    is LinearQLExpr,
    is CountToScoreExpr, is BoolToScoreExpr, is CountToBoolExpr, is AbsoluteDiscountingQLExpr, is BM25Expr, is WeightExpr, is DirQLExpr -> createOptimizedMovementExpr(q.trySingleChild)

    // NOTE: Galago semantics, only look at cond. This is not an AND like you might think. (that's MustExpr)
    is RequireExpr -> createOptimizedMovementExpr(q.cond)

    is DenseFloatField,
    is DenseLongField -> AlwaysMatchLeaf

    // Don't translate these subtrees, as their names give away their behavior! No point in instantiating them.
    AlwaysMatchLeaf, NeverMatchLeaf -> q
}

