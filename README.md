## Irene [![Build Status](https://travis-ci.org/jjfiv/irene.svg?branch=master)](https://travis-ci.org/jjfiv/irene)

Irene is a new query language written on top of Lucene's indexing structures. This query language can be interpreted into both Galago 3.13 and Lucene 7 indexing structures. It may also be run in a learning-to-rank context.

## Getting Started

Clone this repository and then run ``./install_galago``, which will grab Galago as a submodule, and install it (skipping the slow tests) and then you can ``mvn install`` this project (or import to IntelliJ or your favorite IDE).

## Complex Retrieval Models are just Functions

This is our actual definition of Query Likelihood:

```kotlin
fun QueryLikelihood(terms: List<String>, field: String?=null, statsField: String?=null, mu: Double? = null): QExpr {
    return MeanExpr(terms.map { DirQLExpr(TextExpr(it, field, statsField), mu) })
}
```

See more retrieval models, including SDM in [RetrievalModels.kt](https://github.com/jjfiv/irene/blob/master/src/main/kotlin/edu/umass/cics/ciir/irene/lang/RetrievalModels.kt).

Looking for an exact Wikipedia Title match?

```kotlin
// Do we find the exact phrasing, and do we match the length of the field exactly.
fun generateExactMatchQuery(qterms: List<String>, field: String?=null, statsField: String?=null): QExpr {
    return AndExpr(listOf(phraseQuery(qterms, field, statsField), CountEqualsExpr(LengthsExpr(field), qterms.size)))
}
```

Want to only allow documents published after the query document?

```kotlin
val time = doc.published_date ?: 0L
assert(time >= 0L)

val publishedBeforeExpr = LongLTE(DenseLongField("published_date"), time)
val countBefore = index.count(publishedBeforeExpr)
val finalExpr = MustExpr(publishedBeforeExpr, query).deepCopy()
```

## TREC-Core 2018 QL Likelihood Run:

This example gives a sense of how to use the index opening and reading API in order to submit queries and write results to a trec file.

```kotlin
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
```

## Query Language Nodes

Right now, there is no string-based syntax for these Query Language Nodes. They are all either subclasses of ``QExpr`` or they are functions that select sub-classes of ``QExpr`` (e.g., ``MeanExpr`` is a function that sets up weights in a ``CombineExpr``). Because there's no ``new`` keyword in Kotlin, they look the same in a query.

To understand this query language, you need to think of search as logically being done in two passes. In the first pass, we collect all the documents that "match" a query, and in the second pass, we score those documents. Some expressions separate these passes explicitly, like ``MustExpr`` and ``RequireExpr``. Irene is able to optimize these two parts of scoring independently, often leading to much simpler expressions and requirements for document matching even with expensive scoring trees.

### Constants and Match Control
- ``ConstScoreExpr(var x: Double)``. This is a constant value - it never matches any documents.
- ``ConstCountExpr(var x: Int, val lengths: LengthsExpr)``. This is a constant value - it never matches any documents.
- ``ConstBoolExpr(var x: Boolean)``. This is a constant value - it never matches any documents.
- ``AlwaysMatchExpr(child: QExpr) = RequireExpr(AlwaysMatchLeaf, child)``. This wrapper ignores whether or not ``child`` matches any documents and just always considers itself a match. This can be helpful for to extract features for a set of documents no matter what.
- ``NeverMatchExpr(child: QExpr) = RequireExpr(NeverMatchLeaf, child)``. This wrapper ignores whether or not ``child`` matches any documents and always returns false -- this can be used to boost document scores, for instance, but never adds to the pool by itself.
- ``WhitelistMatchExpr(var docNames: Set<String>? = null, var docIdentifiers: List<Int>? = null)``. This is a so-called "working-set" operation. Given a set of internal or external document identifiers, it only scores those in this subtree.
- ``RequireExpr(var cond: QExpr, var value: QExpr)``. A ``RequireExpr`` gives a score from ``value`` sub-expression when ``cond`` matches, regardless of whether the ``value`` expression matches.
- ``MustExpr(var must: QExpr, var value: QExpr)``. A ``MustExpr`` gives a score from the ``value`` sub-expression when both ``must`` and ``value`` match.
 
### Document Metadata and Field Operations
- ``DenseLongField(val name: String, var missing: Long=0L)``. This gives us access to a named long field in each document.
- ``LongLTE(override var child: QExpr, val threshold: Long)``. This gives us the ability to check whether a length or a dense long field is less than or equal to a threshold value.
- ``LengthsExpr(var statsField: String?)``. Given a field to draw statistics from, all documents have a length.
- ``BoolExpr(field: String, desired: Boolean=true)``. We store boolean values in Lucene's ``StringField`` with "T" and "F" to store true and false (we can also detect missing this way). So you don't have to remember those constants with a ``TextEXpr``, we have this node.
- ``TextExpr(var text: String, private var field: String? = null, private var statsField: String? = null, var needed: DataNeeded = DataNeeded.DOCS)``. This is the core of any retrieval model: a term (``text``) located in a given ``field``, with statistics drawn from ``statsField``. The final field ``needed`` is whether presence, counts, or positions are needed to compute the values derived from this term in your expression -- this is calculated automatically by Irene.
- ``LuceneExpr(val rawQuery: String, var query: LuceneQuery? = null )``. Because our primary search system is Lucene, we have the ability to submit Lucene queries in Lucene syntax, and use them at any point in our language. Fill in either ``rawQuery`` (and it will be parsed) or ``LuceneQuery`` which is an alias for ``org.apache.lucene.search.Query``.

### Combination Nodes
- ``SynonymExpr(override var children: List<QExpr>)``
- ``AndExpr(override var children: List<QExpr>)``
- ``OrExpr(override var children: List<QExpr>)``
- ``SumExpr(children: List<QExpr>) = CombineExpr(children, children.map { 1.0 })``
- ``MeanExpr(children: List<QExpr>) = CombineExpr(children, children.map { 1.0 / children.size.toDouble() })``
- ``CombineExpr(override var children: List<QExpr>, var weights: List<Double>)``
- ``MultExpr(override var children: List<QExpr>)``
- ``MaxExpr(override var children: List<QExpr>)``
- ``WeightExpr(override var child: QExpr, var weight: Double = 1.0)``

### Term Dependency Nodes
- ``SmallerCountExpr(override var children: List<QExpr>)``
- ``OrderedWindowExpr(override var children: List<QExpr>, var step: Int=1)``
- ``UnorderedWindowExpr(override var children: List<QExpr>, var width: Int=8)``
- ``ProxExpr(override var children: List<QExpr>, var width: Int=8)``
- ``CountEqualsExpr(override var child: QExpr, var target: Int)``

### Retrieval Model Scorers
- ``DirQLExpr(override var child: QExpr, var mu: Double? = null, var stats: CountStats? = null)``
- ``AbsoluteDiscountingQLExpr(override var child: QExpr, var delta: Double? = null, var stats: CountStats? = null)``
- ``BM25Expr(override var child: QExpr, var b: Double? = null, var k: Double? = null, var stats: CountStats? = null, var extractedIDF: Boolean = false)``

### Change of Type Operations
- ``CountToScoreExpr(override var child: QExpr)``
- ``BoolToScoreExpr(override var child: QExpr, var trueScore: Double=1.0, var falseScore: Double=0.0)``
- ``CountToBoolExpr(override var child: QExpr, var gt: Int = 0)``

