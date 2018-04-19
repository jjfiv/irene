package edu.umass.cics.ciir.irene.lang

/**
 *
 * @author jfoley.
 */

class FixedPointBooleanSimplification() {
    var changed = false
    var iter = 0

    fun optimizeAnd(q: AndExpr): QExpr {
        q.children.find { it is NeverMatchLeaf }?.let {
            changed = true
            return it
        }
        val usefulChildren = ArrayList<QExpr>()
        for (child in q.children) {
            if (child is AlwaysMatchLeaf) {
                changed = true
                continue
            } else if (child is AndExpr) {
                usefulChildren.addAll(child.children)
                changed = true
            } else {
                usefulChildren.add(child)
            }
        }
        return when(usefulChildren.size) {
            0 -> {
                changed = true
                AlwaysMatchExpr(ConstBoolExpr(true))
            }
            1 -> {
                changed = true
                usefulChildren[0]
            }
            else -> AndExpr(usefulChildren)
        }
    }

    fun optimizeOr(q: OrExpr): QExpr {
        q.children.find { it is AlwaysMatchLeaf }?.let {
            changed = true
            return it
        }

        val usefulChildren = ArrayList<QExpr>()
        for (child in q.children) {
            if (child is NeverMatchLeaf) {
                changed = true
                continue
            } else if (child is OrExpr) {
                usefulChildren.addAll(child.children)
                changed = true
            } else {
                usefulChildren.add(child)
            }
        }
        return when(usefulChildren.size) {
            0 -> {
                changed = true
                NeverMatchLeaf
            }
            1 -> {
                changed = true
                usefulChildren[0]
            }
            else -> OrExpr(usefulChildren)
        }
    }

    fun dedupChildren(q: QExpr): List<QExpr>? {
        val uniq= q.children.toSet()
        if (uniq.size < q.children.size) {
            changed = true
            val keep = uniq.toList()

            if (keep.isEmpty()) {
                error("Shouldn't be able to remove duplicates and get to zero! $q")
            }
            return keep
        }
        return null
    }

    /**
     * Find any OR(X ... AND(Y) ) where X subsets Y, and replace with OR(X)
     * This is a specialized optimization for term dependency models, e.g., SDM and FDM, where you have uni *grams (a b c) and bigrams (a b) and (b c).
     * This is the expression OR(a b c AND(a b) AND (b c)) but can be computed cheaper as OR(a b c)
     * a + a'b -> a
     * This will especially help [MultiExpr] pooling situations.
     * This is the implication of rules from boolean algebra:
     * A + AB -> A(1+B) -> A
     * Ergo, the rule is actually more broad than the SDM case: if there exists a term in an AND child of an OR that is also a part of that or, it can be un-distributed out, and the rest of the term cancelled.
    */
    fun removeChildAnds(q: OrExpr): QExpr {
        val childAnds = ArrayList<AndExpr>()
        q.children.forEach {
            if (it is AndExpr) {
                childAnds.add(it)
            }
        }
        if (childAnds.isEmpty()) return q

        val provablyRedundant = childAnds.mapNotNull { andExpr ->
            val topOrChildren = q.children.filterNot { (it === andExpr) }
            val reduce = andExpr.children.any { topOrChildren.contains(it) }
            // Now if we've found a term in a child AND that proves it redundant:
            if (reduce) {
                andExpr
            } else {
                null
            }
        }

        if (provablyRedundant.isEmpty()) return q

        changed = true
        return optimizeOr(OrExpr(q.children.filterNot { provablyRedundant.contains(it) }))
    }

    fun optimizeDuplicates(q: QExpr): QExpr {
        return when (q) {
            is AndExpr -> {
                val nc = dedupChildren(q) ?: return q
                AndExpr(nc)
            }
            is OrExpr -> {
                val nc = dedupChildren(q) ?: return q
                OrExpr(nc)
            }
            is SynonymExpr -> {
                val nc = dedupChildren(q) ?: return q
                SynonymExpr(nc)
            }
            is MaxExpr -> {
                val nc = dedupChildren(q) ?: return q
                MaxExpr(nc)
            }
            is SmallerCountExpr -> {
                val nc = dedupChildren(q) ?: return q
                SmallerCountExpr(nc)
            }
            else -> return q
        }
    }
}

fun simplifyBools(input: QExpr, ctx: FixedPointBooleanSimplification): QExpr {
    return ctx.optimizeDuplicates(input).map { q ->
        when (q) {
        // AND and OR
            is AndExpr -> ctx.optimizeAnd(q)
            is OrExpr -> {
                val opt = ctx.optimizeOr(q)
                if (opt is OrExpr) {
                    ctx.removeChildAnds(opt)
                } else {
                    opt
                }
            }

        // Leaf expr:
            is ConstBoolExpr,
            is ConstCountExpr,
            is ConstScoreExpr,
            is LengthsExpr,
            is LuceneExpr,
            NeverMatchLeaf,
            AlwaysMatchLeaf,
            is TextExpr -> q

        // default, just pass through:
            NeverMatchLeaf,
            is AbsoluteDiscountingQLExpr,
            is BM25Expr,
            is BoolToScoreExpr,
            is CombineExpr,
            is CountToBoolExpr,
            is CountToScoreExpr,
            is CountEqualsExpr,
            is DirQLExpr,
            is MaxExpr,
            is MultExpr,
            is OrderedWindowExpr,
            is ProxExpr,
            is RequireExpr,
            is SmallerCountExpr,
            is SynonymExpr,
            is UnorderedWindowCeilingExpr,
            is UnorderedWindowExpr,
            is WeightExpr,
            is WhitelistMatchExpr,
            is MultiExpr -> q
        }
    }
}

fun simplifyBooleanExpr(q: QExpr): QExpr? {
    var changed = false
    val ctx = FixedPointBooleanSimplification()
    ctx.changed = true
    var rq = q
    while(ctx.changed) {
        ctx.changed = false
        rq = simplifyBools(rq, ctx)
        ctx.iter++
        changed = changed || ctx.changed
    }
    if (changed) return rq
    return null
}
