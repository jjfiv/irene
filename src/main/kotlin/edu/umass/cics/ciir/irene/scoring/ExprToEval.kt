package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.lang.*
import gnu.trove.set.hash.TIntHashSet
import org.apache.lucene.index.Term

/**
 * This function translates the public-facing query language ([QExpr] and subclasses) to a set of private-facing operators ([QueryEvalNode] and subclasses).
 * @author jfoley.
 */
fun exprToEval(q: QExpr, ctx: EvalSetupContext): QueryEvalNode = when(q) {
    is TextExpr -> try {
        ctx.create(Term(q.countsField(), q.text), q.needed)
    } catch (e: Exception) {
        println(q)
        throw e
    }
    is LuceneExpr -> TODO()
    is SynonymExpr -> TODO()
    is AndExpr -> BooleanAndEval(q.children.map { exprToEval(it, ctx) })
    is OrExpr -> BooleanOrEval(q.children.map { exprToEval(it, ctx) })
    is CombineExpr -> if(ctx.env.optimizeDirLog && q.children.all { it is DirQLExpr }) {
        // Ok, if this is query likelihood, optimize code gen:
        WeightedLogSumEval(
                q.children.map { c ->
                    val d = c as DirQLExpr
                    NoLogDirichletSmoothingEval(
                            exprToEval(c.child, ctx),
                            ctx.createLengths(c.child.getLengthsField()),
                            d.mu!!, c.stats!!)
                },
                q.weights.map { it }.toDoubleArray())
    } else {
        WeightedSumEval(
                q.children.map { exprToEval(it, ctx) },
                q.weights.map { it }.toDoubleArray())
    }
    is MultExpr -> TODO()
    is MaxExpr -> MaxEval(q.children.map { exprToEval(it, ctx) })
    is WeightExpr -> WeightedEval(exprToEval(q.child, ctx), q.weight)
    is DirQLExpr -> {
        DirichletSmoothingEval(
                exprToEval(q.child, ctx),
                ctx.createLengths(q.child.getLengthsField()),
                q.mu!!, q.stats!!)
    }
    is BM25Expr -> if (q.extractedIDF) {
        BM25InnerScoringEval(
                exprToEval(q.child, ctx),
                ctx.createLengths(q.child.getLengthsField()),
                q.b!!, q.k!!, q.stats!!)
    } else {
        BM25ScoringEval(
                exprToEval(q.child, ctx),
                ctx.createLengths(q.child.getLengthsField()),
                q.b!!, q.k!!, q.stats!!)
    }
    is CountToScoreExpr -> TODO()
    is BoolToScoreExpr -> TODO()
    is CountToBoolExpr -> TODO()
    is RequireExpr -> RequireEval(exprToEval(q.cond, ctx), exprToEval(q.value, ctx))
    is OrderedWindowExpr -> OrderedWindow(
            q.children.map { exprToEval(it, ctx) as PositionsEvalNode }, q.step)
    is UnorderedWindowExpr -> UnorderedWindow(
            q.children.map { exprToEval(it, ctx) as PositionsEvalNode }, q.width)
    is ProxExpr -> ProxWindow(
            q.children.map { exprToEval(it, ctx) as PositionsEvalNode }, q.width)
    is SmallerCountExpr -> SmallerCountWindow(
            q.children.map { exprToEval(it, ctx) })
    is UnorderedWindowCeilingExpr -> UnorderedWindowCeiling(
            q.width,
            q.children.map { exprToEval(it, ctx) })
    is ConstScoreExpr -> ConstEvalNode(q.x)
    is ConstCountExpr -> ConstCountEvalNode(q.x, exprToEval(q.lengths, ctx))
    is ConstBoolExpr -> if(q.x) ConstTrueNode(ctx.numDocs()) else ConstEvalNode(0)
    is AbsoluteDiscountingQLExpr -> TODO("No efficient method to implement AbsoluteDiscountingQLExpr in Irene backend; needs numUniqWords per document.")
    is MultiExpr -> MultiEvalNode(q.children.map { exprToEval(it, ctx) }, q.names)
    is LengthsExpr -> ctx.createLengths(q.statsField ?: error("statsField not set"))
    NeverMatchLeaf -> FixedMatchEvalNode(false, 0)
    AlwaysMatchLeaf -> FixedMatchEvalNode(true, ctx.numDocs().toLong())
    is WhitelistMatchExpr -> WhitelistMatchEvalNode(TIntHashSet(ctx.selectRelativeDocIds(q.docIdentifiers!!)))
    is CountEqualsExpr -> CountEqualsNode(q.target, exprToEval(q.child, ctx))
}

