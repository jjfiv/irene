package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.DataNeeded
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.createOptimizedMovementExpr
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.RREnv
import edu.umass.cics.ciir.irene.lucene_try
import edu.umass.cics.ciir.irene.utils.ComputedStats
import edu.umass.cics.ciir.irene.utils.IntList
import edu.umass.cics.ciir.irene.utils.StreamingStats
import org.apache.lucene.index.*
import org.apache.lucene.search.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 *
 * @author jfoley.
 */

interface EvalSetupContext {
    val env : RREnv
    fun create(term: Term, needed: DataNeeded): QueryEvalNode
    fun createLengths(field: String): QueryEvalNode
    fun numDocs(): Int
    fun selectRelativeDocIds(ids: List<Int>): IntList
}

data class IQContext(val iqm: IreneQueryModel, val context: LeafReaderContext) : EvalSetupContext {
    override val env = iqm.env
    val searcher = iqm.index.searcher
    val lengths = HashMap<String, NumericDocValues>()
    val iterNeeds = HashMap<Term, DataNeeded>()
    val evalCache = HashMap<Term, QueryEvalNode>()

    private fun getLengths(field: String) = lengths.computeIfAbsent(field, { missing -> getLengthsRaw(missing) })

    private fun getLengthsRaw(field: String): NumericDocValues {
        // Get explicitly indexed lengths:
        val lengths = lucene_try { context.reader().getNumericDocValues("lengths:$field") }
                // Or get the norms... lucene doesn't always put lengths here...
                //?: lucene_try { context.reader().getNormValues(field) }
                // Or crash.
                ?: error("Couldn't find norms for ``$field'' ND=${context.reader().numDocs()} F=${context.reader().fieldInfos.map { it.name }}.")

        // Lucene requires that we call nextDoc() to begin reading any DocIdSetIterator
        lengths.nextDoc()
        return lengths
    }

    override fun create(term: Term, needed: DataNeeded): QueryEvalNode {
        return create(term, needed, getLengths(term.field()))
    }

    override fun selectRelativeDocIds(ids: List<Int>): IntList {
        val base = context.docBase
        val limit = base + context.reader().numDocs()
        val output = IntList()
        ids.forEach { id ->
            if(id >= base && id < limit) {
                output.push(id-base)
            }
        }
        output.sort()
        return output
    }

    private fun termRaw(term: Term, needed: DataNeeded, lengths: NumericDocValues): QueryEvalNode {
        val postings = termSeek(term, needed)
                ?: return LuceneMissingTerm(term)
        return when(needed) {
            DataNeeded.DOCS -> LuceneTermDocs(term, postings)
            DataNeeded.COUNTS -> LuceneTermCounts(term, postings)
            DataNeeded.POSITIONS -> LuceneTermPositions(term, postings)
            DataNeeded.SCORES -> error("Impossible!")
        }
    }

    private fun termCached(term: Term, needed: DataNeeded, lengths: NumericDocValues): QueryEvalNode {
        return if (env.shareIterators) {
            val entry = iterNeeds[term]
            if (entry == null || entry < needed) {
                error("Forgot to call .setup(q) on IQContext. While constructing $term $needed $entry...")
            }
            evalCache.computeIfAbsent(term, { termRaw(term, entry, lengths) })
        } else {
            termRaw(term, needed, lengths)
        }
    }
    fun termMover(term: Term): QueryMover {
        val result = termSeek(term, DataNeeded.DOCS) ?: return NeverMatchMover(term.toString())

        return LuceneDocsMover(term.toString(), result)
    }
    private fun termSeek(term: Term, needed: DataNeeded): PostingsEnum? {
        val termContext = TermContext.build(searcher.topReaderContext, term)!!
        val state = termContext[context.ord] ?: return null
        val termIter = context.reader().terms(term.field()).iterator()
        termIter.seekExact(term.bytes(), state)
        val postings = termIter.postings(null, needed.textFlags())
        // Lucene requires we call nextDoc() before doing anything else.
        postings.nextDoc()
        return postings
    }

    fun create(term: Term, needed: DataNeeded, lengths: NumericDocValues): QueryEvalNode {
        return termCached(term, needed, lengths)
    }
    fun shardIdentity(): Any = context.id()
    override fun numDocs(): Int = context.reader().numDocs()

    override fun createLengths(field: String): CountEvalNode {
        return LuceneDocLengths(field, getLengths(field), getLengthsInfo(field))
    }

    fun setup(input: QExpr): QExpr {
        val foldOperators = if (!env.indexedBigrams) {
            input
        } else {
            input.map { q ->
                if (q is OrderedWindowExpr && q.children.size == 2) {
                    val lhs = q.children[0] as TextExpr
                    val rhs = q.children[1] as TextExpr
                    TextExpr("${lhs.text} ${rhs.text}", field="od:${lhs.countsField()}", statsField = "ERROR_PRECOMPUTED_STATS", needed = DataNeeded.COUNTS)
                } else q
            }
        }

        if (env.shareIterators) {
            foldOperators.findTextNodes().map { q ->
                val term = Term(q.countsField(), q.text)
                val needed = q.needed
                iterNeeds.compute(term, { missing, prev ->
                    if (prev == null) {
                        needed
                    } else {
                        maxOf(prev, needed)
                    }
                })
            }
        }

        return foldOperators
    }

    fun getLengthsInfo(field: String): ComputedStats {
        return lengthInfoCache.computeIfAbsent(Pair(field, this.shardIdentity()), {
            val stats = StreamingStats()
            val lengths = getLengthsRaw(field)
            while(lengths.docID() < NO_MORE_DOCS) {
                stats.push(lengths.longValue())
                lengths.nextDoc()
            }
            stats.toComputedStats()
        })
    }

    companion object {
        val lengthInfoCache = ConcurrentHashMap<Pair<String, Any>, ComputedStats>()
    }
}

private class IQModelWeight(val q: QExpr, val m: QExpr, val iqm: IreneQueryModel) : Weight(iqm) {
    override fun extractTerms(terms: MutableSet<Term>) {
        TODO("not implemented")
    }

    override fun explain(context: LeafReaderContext, doc: Int): Explanation {
        val ctx = IQContext(iqm, context)
        val rq = ctx.setup(q)
        val eval = exprToEval(rq, ctx)
        return eval.explain(ScoringEnv(doc))
    }

    override fun scorer(context: LeafReaderContext): Scorer {
        val ctx = IQContext(iqm, context)
        val rq = ctx.setup(q)
        val qExpr = exprToEval(rq, ctx)
        val mExpr = createMover(m, ctx)
        //println("mExpr: $mExpr")
        //println("qExpr: ${qExpr}")
        val iter = OptimizedMovementIter(mExpr, qExpr)
        return IreneQueryScorer(q, iter)
    }
}

class OptimizedMovementIter(val movement: QueryMover, val score: QueryEvalNode): DocIdSetIterator() {
    val env = ScoringEnv()
    init {
        env.doc = advance(0)
    }
    override fun docID(): Int = movement.docID()
    override fun nextDoc(): Int = advance(docID()+1)
    override fun cost(): Long = movement.estimateDF()
    override fun advance(target: Int): Int {
        var dest = target
        while (!movement.done) {
            val nextMatch = movement.nextMatching(dest)
            if (nextMatch == NO_MORE_DOCS) break
            env.doc = nextMatch
            //println("Consider candidate: $nextMatch matching... ${score.matches()} ${score.explain()}")
            if (score.matches(env)) {
                return nextMatch
            } else {
                dest = maxOf(dest+1, nextMatch+1)
            }
        }
        return NO_MORE_DOCS
    }
    fun score(doc: Int): Float {
        env.doc = doc
        return score.score(env).toFloat()
    }
    fun count(doc: Int): Int {
        env.doc = doc
        return score.count(env)
    }
}

class IreneQueryScorer(val q: QExpr, val iter: OptimizedMovementIter) : Scorer(null) {
    val eval: QueryEvalNode = iter.score
    val env: ScoringEnv = iter.env
    override fun docID(): Int {
        val docid = iter.docID()
        //println(docid)
        return docid
    }
    override fun iterator(): DocIdSetIterator = iter
    override fun score(): Float {
        val returned = iter.score(docID())
        if (java.lang.Float.isInfinite(returned)) {
            throw RuntimeException("Infinite response for document: ${iter.docID()}@${env.doc} ${eval.explain(env)}")
            //return -Float.MAX_VALUE
        }
        return returned
    }
    override fun freq(): Int = iter.count(docID())
}

class IreneQueryModel(val index: IreneIndex, val env: IreneQueryLanguage, q: QExpr) : LuceneQuery() {
    val exec = env.prepare(q)
    var movement = if (env.optimizeMovement) {
        val m = env.prepare(createOptimizedMovementExpr(exec))
        m
    } else {
        exec
    }

    override fun createWeight(searcher: IndexSearcher?, needsScores: Boolean, boost: Float): Weight {
        return IQModelWeight(exec, movement, this)
    }
    override fun hashCode(): Int {
        return exec.hashCode()
    }
    override fun equals(other: Any?): Boolean {
        if (other is IreneQueryModel) {
            return exec == other.exec
        }
        return false
    }
    override fun toString(field: String?): String {
        return exec.toString()
    }
}

inline fun NumericDocValues.forEach(f: (Long)->Unit) {
    val current = docID()
    // Leave if finished.
    if (current == NO_MORE_DOCS) return
    // Start if not yet ready.
    if (current == -1) nextDoc()

    while(docID() < NO_MORE_DOCS) {
        f(longValue())
        nextDoc()
    }
}

