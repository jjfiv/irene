package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.lucene.*

/**
 * Someday make this two-passes. For now, depend on the cache in the index to make it fast-enough.
 * @author jfoley.
 */
fun insertStats(env: RREnv, input: QExpr) = qmap(input) { q ->
    when(q) {
        AlwaysMatchLeaf,
        NeverMatchLeaf,
        is AndExpr,
        is BoolToScoreExpr,
        is CombineExpr,
        is ConstBoolExpr,
        is ConstCountExpr,
        is ConstScoreExpr,
        is CountEqualsExpr,
        is CountToBoolExpr,
        is CountToScoreExpr,
        is LuceneExpr,
        is MaxExpr,
        is MultExpr,
        is MultiExpr,
        is OrExpr,
        is OrderedWindowExpr,
        is ProxExpr,
        is RequireExpr,
        is MustExpr,
        is SmallerCountExpr,
        is SynonymExpr,
        is TextExpr,
        is UnorderedWindowCeilingExpr,
        is UnorderedWindowExpr,
        is WeightExpr,
        is LogValueExpr,
        is DenseFloatField,
        is DenseLongField,
        is LongLTE,
        is WhitelistMatchExpr -> q

        is LengthsExpr -> LengthsExpr(q.statsField!!)
        is LinearQLExpr -> LinearQLExpr(q.trySingleChild, q.lambda, q.stats ?: computeCountStats(q.trySingleChild, env).get())
        is DirQLExpr -> DirQLExpr(q.trySingleChild, q.mu, q.stats ?: computeCountStats(q.trySingleChild, env).get())
        is AbsoluteDiscountingQLExpr -> AbsoluteDiscountingQLExpr(q.trySingleChild, q.delta, q.stats ?: computeCountStats(q.trySingleChild, env).get())
        is BM25Expr -> BM25Expr(q.trySingleChild, b=q.b, k=q.k, stats=q.stats ?: computeCountStats(q.trySingleChild, env).get())

        is RM3Expr -> error("Expansion Models should not be created directly!")
    }
}

fun approxStats(env: RREnv, q: QExpr, method: String): CountStatsStrategy {
    if (q is OrderedWindowExpr || q is UnorderedWindowExpr || q is SmallerCountExpr || q is UnorderedWindowCeilingExpr || q is ProxExpr) {
        val cstats = q.children.map { c ->
            if (c is TextExpr) {
                c.getStats(env)
            } else {
                error("Can't estimate stats with non-TextExpr children. $c")
            }
        }
        return when(method) {
            "min" -> MinEstimatedCountStats(q.deepCopy(), cstats)
            "prob" -> ProbEstimatedCountStats(q.deepCopy(), cstats)
            "exact" -> LazyCountStats(q, env)
            else -> TODO("estimateStats strategy = $method")
        }
    } else {
        TODO("approxStats($q)")
    }
}

fun computeCountStats(q: QExpr, env: RREnv): CountStatsStrategy {
    return if (q is TextExpr) {
        ExactEnvStats(env, q.text, q.statsField())
    } else if (q is OrderedWindowExpr || q is UnorderedWindowExpr || q is SmallerCountExpr || q is UnorderedWindowCeilingExpr || q is ProxExpr) {
        val method = env.config.estimateStats ?: return LazyCountStats(q.deepCopy(), env)
        approxStats(env, q, method)
    } else {
        TODO("computeCountStats($q)")
    }
}
