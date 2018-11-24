## Irene

Irene is a new query language written on top of Lucene's indexing structures. This query language can be interpreted into both Galago 3.13 and Lucene 7 indexing structures. It may also be run in a learning-to-rank context.

## Complex Retrieval Models are just Functions

This is our actual definition of Query Likelihood:

    fun QueryLikelihood(terms: List<String>, field: String?=null, statsField: String?=null, mu: Double? = null): QExpr {
        return MeanExpr(terms.map { DirQLExpr(TextExpr(it, field, statsField), mu) })
    }

Looking for an exact Wikipedia Title match?

    // Do we find the exact phrasing, and do we match the length of the field exactly.
    fun generateExactMatchQuery(qterms: List<String>, field: String?=null, statsField: String?=null): QExpr {
        return AndExpr(listOf(phraseQuery(qterms, field, statsField), CountEqualsExpr(LengthsExpr(field), qterms.size)))
    }

Want to only allow documents published after the query document?

    val time = doc.published_date ?: 0L
    assert(time >= 0L)

    val publishedBeforeExpr = LongLTE(DenseLongField("published_date"), time)
    val countBefore = index.count(publishedBeforeExpr)
    val finalExpr = MustExpr(publishedBeforeExpr, query).deepCopy()

## TREC-Core 2018 QL Likelihood Run:

This example gives a sense of how to use the index opening and reading API in order to submit queries and write results to a trec file.

    fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val queryFile = argp.get("queryFile", "${System.getenv("HOME")}/code/queries/trec_core/2018-test-topics.txt")
        val queries = LoadTrecCoreQueries(queryFile)
        val qlOutput = File("umass_ql.trecrun.gz").smartPrinter()

        IndexParams().apply {
            withPath(File(argp.get("index", defaultWapoIndexPath)))
        }.openReader().use { index ->
            index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)

            for (q in queries.values) {
                val titleTerms = index.tokenize(q.title)
                println("${q.qid} ${titleTerms.joinToString(separator=" ")}")
                val ql = QueryLikelihood(titleTerms)
                val (time, scores) = timed { index.search(ql, 10000) }
                System.out.printf("\tQL Time: %1.3fs total=%d\n", time, scores.totalHits)
                scores.toQueryResults(index, qid = q.qid).outputTrecrun(qlOutput, "umass_ql")
            }
        }

        qlOutput.close()
    }

## Query Language Nodes

### Constants and Match Control
- ConstScoreExpr(var x: Double)
- ConstCountExpr(var x: Int, val lengths: LengthsExpr)
- ConstBoolExpr(var x: Boolean)
- AlwaysMatchExpr(child: QExpr) = RequireExpr(AlwaysMatchLeaf, child)
- AlwaysMatchLeaf
- NeverMatchExpr(child: QExpr) = RequireExpr(NeverMatchLeaf, child)
- NeverMatchLeaf
- WhitelistMatchExpr(var docNames: Set<String>? = null, var docIdentifiers: List<Int>? = null)
- RequireExpr(var cond: QExpr, var value: QExpr)
- MustExpr(var must: QExpr, var value: QExpr)

### Document Metadata and Field Operations
- DenseLongField(val name: String, var missing: Long=0L)
- LongLTE(override var child: QExpr, val threshold: Long)
- LengthsExpr(var statsField: String?)
- BoolExpr(field: String, desired: Boolean=true)
- TextExpr(var text: String, private var field: String? = null, private var statsField: String? = null, var needed: DataNeeded = DataNeeded.DOCS)
- LuceneExpr(val rawQuery: String, var query: LuceneQuery? = null )

### Combination Nodes
- SynonymExpr(override var children: List<QExpr>)
- AndExpr(override var children: List<QExpr>)
- OrExpr(override var children: List<QExpr>)
- SumExpr(children: List<QExpr>) = CombineExpr(children, children.map { 1.0 })
- MeanExpr(children: List<QExpr>) = CombineExpr(children, children.map { 1.0 / children.size.toDouble() })
- CombineExpr(override var children: List<QExpr>, var weights: List<Double>)
- MultExpr(override var children: List<QExpr>)
- MaxExpr(override var children: List<QExpr>)
- WeightExpr(override var child: QExpr, var weight: Double = 1.0)

### Term Dependency Nodes
- SmallerCountExpr(override var children: List<QExpr>)
- OrderedWindowExpr(override var children: List<QExpr>, var step: Int=1)
- UnorderedWindowExpr(override var children: List<QExpr>, var width: Int=8)
- ProxExpr(override var children: List<QExpr>, var width: Int=8)
data class CountEqualsExpr(override var child: QExpr, var target: Int)

### Retrieval Model Scorers
data class DirQLExpr(override var child: QExpr, var mu: Double? = null, var stats: CountStats? = null)
data class AbsoluteDiscountingQLExpr(override var child: QExpr, var delta: Double? = null, var stats: CountStats? = null)
data class BM25Expr(override var child: QExpr, var b: Double? = null, var k: Double? = null, var stats: CountStats? = null, var extractedIDF: Boolean = false)

### Change of Type Operations
- CountToScoreExpr(override var child: QExpr)
- BoolToScoreExpr(override var child: QExpr, var trueScore: Double=1.0, var falseScore: Double=0.0)
- CountToBoolExpr(override var child: QExpr, var gt: Int = 0)

