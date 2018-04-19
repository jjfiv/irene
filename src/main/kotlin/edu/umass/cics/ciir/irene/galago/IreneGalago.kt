package edu.umass.cics.ciir.irene.galago

import edu.umass.cics.ciir.irene.DataNeeded
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.RREnv
import org.apache.lucene.search.TopDocs
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.SimpleEvalDoc
import org.lemurproject.galago.core.retrieval.Retrieval
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters

/**
 *
 * @author jfoley.
 */

fun TopDocs.toQueryResults(index: IreneIndex, qid: String = "dont-care") = QueryResults(qid, this.scoreDocs.mapIndexed { i, sdoc ->
    val name = index.getDocumentName(sdoc.doc) ?: "null"
    SimpleEvalDoc(name, i+1, sdoc.score.toDouble())
})

fun GExpr.transform(ret: Retrieval): GExpr = ret.transformQuery(this, Parameters.create())!!

fun QExpr.toGalago(env: RREnv): GExpr = toGalagoRecursive(env.prepare(this.deepCopy()))
private fun createLengths(child: QExpr): GExpr {
    val fields = child.getStatsFields()
    when(fields.size) {
        0 -> return GExpr("lengths")
        1 -> return GExpr("lengths").apply { setf("field", fields.first()) }
        else -> error("Cannot create Galago #lengths for child: $child.\nHas many fields: $fields")
    }
}

private fun children(q: QExpr) = q.children.map { toGalagoRecursive(it) }

private fun toGalagoRecursive(q : QExpr): GExpr {
    return when (q) {
        is TextExpr -> {
            val operator = when(q.needed) {
                DataNeeded.DOCS, DataNeeded.COUNTS -> "counts"
                DataNeeded.POSITIONS -> "extents"
                DataNeeded.SCORES -> TODO()
            }
            GExpr(operator, q.text).apply {
                setf("field", q.countsField())
            }
        }
        is SynonymExpr -> GExpr("syn", children(q))
        is AndExpr -> GExpr("band", children(q))
        is OrExpr -> GExpr("bor", children(q))
        is CombineExpr -> GExpr("combine", children(q)).apply {
            setf("norm", false)
            q.weights.forEachIndexed { i, w ->
                setf("$i", w)
            }
        }
        is MultExpr -> GExpr("wsum", children(q)).apply {
            setf("norm", false)
        }
        is OrderedWindowExpr -> GExpr("od", children(q)).apply {
            setf("default", q.step)
        }
        is UnorderedWindowExpr -> GExpr("uw", children(q)).apply {
            setf("default", q.width)
        }
        is WeightExpr -> GExpr("combine", toGalagoRecursive(q.child)).apply {
            setf("norm", false)
            setf("0", q.weight)
        }
        is DirQLExpr -> GExpr("dirichlet").apply {
            addChild(createLengths(q.child))
            addChild(toGalagoRecursive(q.child))
            setf("defaultDirichletMu", q.mu)
        }
        is BM25Expr -> GExpr("bm25").apply {
            // LENGTHS MUST BE FIRST.
            addChild(createLengths(q.child))
            addChild(toGalagoRecursive(q.child))
            setf("b", q.b)
            setf("k", q.k)
        }
        is BoolToScoreExpr -> GExpr("bool", children(q))
        is RequireExpr -> GExpr("require", listOf(toGalagoRecursive(q.cond), toGalagoRecursive(q.value)))
        is MaxExpr -> TODO()
        is CountToBoolExpr -> TODO()
        is CountToScoreExpr -> TODO()
        is ConstScoreExpr, is ConstCountExpr, is ConstBoolExpr -> error("Galago does not support constants in its queries.")
        is LuceneExpr -> error("Can never support LuceneExpr -> Galago Query.")
        is MultiExpr -> error("Can not support MultiExpr as Galago Query without custom ProcessingModel.")
        is AbsoluteDiscountingQLExpr -> TODO()
        is UnorderedWindowCeilingExpr, is SmallerCountExpr -> TODO()
        // TODO, allow stats-hacking:
        is LengthsExpr -> GExpr("lengths").apply { setf("field", q.statsField) }
        AlwaysMatchLeaf -> TODO()
        NeverMatchLeaf -> TODO()
        is WhitelistMatchExpr -> TODO()
        is ProxExpr -> TODO()
        is CountEqualsExpr -> TODO()
    }
}

fun TopDocs.logSumExp(): Double = if (this.scoreDocs.isNotEmpty()) MathUtils.logSumExp(this.scoreDocs.map { it.score.toDouble() }.toDoubleArray()) else 0.0

