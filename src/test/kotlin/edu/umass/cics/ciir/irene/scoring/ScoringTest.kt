package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.galago.*
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.RREnv
import edu.umass.cics.ciir.irene.ltr.toRRExpr
import edu.umass.cics.ciir.irene.tokenize
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.junit.Assert
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.ExternalResource
import org.lemurproject.galago.core.index.mem.MemoryIndex
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.core.retrieval.iterator.CountIterator
import org.lemurproject.galago.utility.Parameters
import java.io.Closeable
import java.util.*

/**
 * @author jfoley.
 */

fun <T> MutableMap<T,Int>.incr(x: T, amt: Int=1) {
    this.compute(x, { _, prev -> (prev ?: 0) + amt })
}

class CommonTestIndexes : Closeable {
    val doc1 = "the quick brown fox jumped over the lazy dog";
    val doc2 = "language modeling is the best";
    val doc3 = "the fox jumped the language of the brown dog";

    val idField = "id"
    val docNumberField = "docNo"
    // This is Galago's default field name, which is required if we want to share stats / prepare with one and execute with the other.
    val contentsField = "document"

    val docs = listOf(doc1,doc2,doc3)
    val names = ArrayList<String>()
    val ND: Int get() = names.size
    val gMemIndex = MemoryIndex(pmake {
        set("nonstemming", false)
        set("corpus", true)
    })
    val ltrIndex = ArrayList<LTRDoc>()
    val df = HashMap<String, Int>()
    val terms = HashSet<String>()
    val tokenVectors = ArrayList<List<String>>()

    lateinit var irene: IreneIndex
    lateinit var galago: LocalRetrieval
    lateinit var env: RREnv
    init {
        val params = IndexParams().apply {
            inMemory()
            defaultField = contentsField
            defaultAnalyzer = WhitespaceAnalyzer()
        }

        IreneIndexer(params).use { writer ->
            (0 until 10).forEach {
                val shuf = docs.toMutableList()
                Collections.shuffle(shuf)
                docs.forEachIndexed { num, doc ->
                    val name = "doc${names.size}"
                    names.add(name)

                    writer.doc {
                        setId(name)
                        setTextField(contentsField, doc)
                    }

                    val tokens = params.analyzer.tokenize("body", doc)
                    tokenVectors.add(tokens)

                    val gdoc = GDoc()
                    gdoc.name = name
                    gdoc.terms = tokens
                    gdoc.text = doc
                    gdoc.tags = emptyList()
                    gMemIndex.process(gdoc)

                    ltrIndex.add(LTRDoc(name, doc, contentsField))

                    terms.addAll(tokens)
                    tokens.toSet().forEach {
                        df.incr(it)
                    }
                }
            }
            writer.commit()
            irene = writer.open()
            galago = LocalRetrieval(gMemIndex)
            env = RRGalagoEnv(galago)
            env.estimateStats = "exact"
        }
    }

    fun forEachTermPair(fn: (String,String) -> Unit) {
        val linear = terms.toList()
        linear.indices.forEach { i ->
            linear.indices.forEach { j ->
                if (i != j) {
                    fn(linear[i], linear[j])
                }
            }
        }
    }

    override fun close() {
        galago.close()
        ltrIndex.clear()
        irene.close()
    }
}

class CTIResource : ExternalResource() {
    var index: CommonTestIndexes? = null
    override fun before() { index = CommonTestIndexes() }
    override fun after() { index?.close() }
}

class ScoringTest {
    companion object {
        @ClassRule @JvmField
        val resource = CTIResource()
    }

    val EPSILON = 0.0001
    inline fun dblEquals(x: Double, y: Double, orElse: ()->Unit) {
       if (Math.abs(x-y) > EPSILON) {
           orElse()
           Assert.assertEquals(x, y, EPSILON)
       }
    }

    @Test
    fun testAllStats() {
        val index = resource.index!!
        val bgStats = index.galago.getCollectionStatistics(GExpr("lengths"))
        index.terms.forEach { term ->
            val istats = index.irene.getStats(term)
            val gstats = (index.galago.createIterator(pmake {}, GExpr("counts", term)) as CountIterator).calculateStatistics()
            //println(gstats)
            Assert.assertEquals("cf $term", gstats.nodeFrequency, istats.cf)
            Assert.assertEquals("df $term", gstats.nodeDocumentCount, istats.df)
            Assert.assertEquals("dc $term", bgStats.documentCount, istats.dc)
            Assert.assertEquals("cl $term", bgStats.collectionLength, istats.cl)
        }
    }

    @Test
    fun testBigramStats() {
        val index = resource.index!!
        val bgStats = index.galago.getCollectionStatistics(GExpr("lengths"))
        index.forEachTermPair { t1, t2 ->
            val odi = OrderedWindowExpr(listOf(TextExpr(t1), TextExpr(t2)))
            val odg = odi.toGalago(index.env)
            //val odg = galagoOd1(listOf(t1, t2));
            val istats = index.irene.getStats(odi)
            val gstats = index.galago.getNodeStatistics(index.galago.transformQuery(odg, Parameters.create()));
            // Galago does this wrong!
            var df = 0L
            var cf = 0L
            index.tokenVectors.forEach { tvec ->
                var hits = 0
                (0 until tvec.size-1).forEach { i ->
                    if (tvec[i] == t1 && tvec[i+1] == t2)
                        hits++
                }
                if (hits > 0) df++
                cf+=hits
            }

            val term = "od:1($t1 $t2)"
            Assert.assertEquals("cf $term", cf, istats.cf)
            Assert.assertEquals("df $term", df, istats.df)
            Assert.assertEquals("g-cf $term", cf, gstats.nodeFrequency)
            Assert.assertEquals("g-df $term", df, gstats.nodeDocumentCount)
            Assert.assertEquals("dc $term", bgStats.documentCount, istats.dc)
            Assert.assertEquals("cl $term", bgStats.collectionLength, istats.cl)
        }
    }

    @Test
    fun testWindowStats() {
        val index = resource.index!!

        val bgStats = index.galago.getCollectionStatistics(GExpr("lengths"))
        index.forEachTermPair { t1, t2 ->
            listOf(3,6,9).forEach { width ->
                val udi = UnorderedWindowExpr(listOf(TextExpr(t1), TextExpr(t2)), width)
                val udg = udi.toGalago(index.env)
                val istats = index.irene.getStats(udi)
                val gstats = index.galago.getNodeStatistics(index.galago.transformQuery(udg, Parameters.create()));

                val term = "uw:$width($t1 $t2)"
                Assert.assertEquals("cf $term", gstats.nodeFrequency, istats.cf)
                Assert.assertEquals("df $term", gstats.nodeDocumentCount, istats.df)
                Assert.assertEquals("dc $term", bgStats.documentCount, istats.dc)
                Assert.assertEquals("cl $term", bgStats.collectionLength, istats.cl)
            }
        }
    }

    @Test
    fun testBigramScores() {
        val index = resource.index!!

        index.forEachTermPair { t1, t2 ->
            if (t1 != "over" && t2 != "quick") return@forEachTermPair
            val odi = DirQLExpr(OrderedWindowExpr(listOf(TextExpr(t1), TextExpr(t2))))
            val odg = odi.toGalago(index.env)
            cmpResults("dirichlet.od($t1,$t2)", odg, odi, index)
        }
    }

    fun cmpResults(str: String, gq: GExpr, iq: QExpr, index: CommonTestIndexes) {
        val search = index.irene.search(iq, index.ND)
        val gres = index.galago.transformAndExecuteQuery(gq, pmake {
            set("processingModel", "rankeddocument")
            set("annotate", true)
            set("requested", index.ND)
        })
        val gTruth = gres.scoredDocuments.associate { Pair(it.name, it) }

        if (search.totalHits != gTruth.size.toLong()) {
            val found = search.scoreDocs.map { it.doc }.toSet()
            println("Irene Found: ${found}")
            println("Galago Found: ${gTruth.keys}")
            gTruth.values.forEach { sdoc ->
                val id = index.irene.documentById(sdoc.name)!!
                if (!found.contains(id)) {
                    println("Can't score document $id correctly... $str")
                    println(sdoc.annotation)
                    println(index.irene.explain(iq, id))
                }
            }
        }

        Assert.assertEquals(str, gTruth.size.toLong(), search.totalHits)

        search.scoreDocs.forEach { ldoc ->
            val name = index.irene.getDocumentName(ldoc.doc)!!
            val sameDoc = gTruth[name] ?: error("Galago returned different documents! $str")

            Assert.assertEquals(index.names[ldoc.doc], name)
            dblEquals(ldoc.score.toDouble(), sameDoc.score) {
                println(sameDoc.annotation)
                println(index.irene.explain(iq, ldoc.doc))
            }
        }
    }

    @Test
    fun testEquivQL() {
        val index = resource.index!!

        index.forEachTermPair { t1, t2 ->
            val iq = QueryLikelihood(listOf(t1, t2))
            val gq = iq.toGalago(index.env)
            cmpResults("$t1, $t2", gq, iq, index)
        }
    }

    @Test
    fun testEquivWithMissing() {
        val index = resource.index!!

        index.forEachTermPair { t1, t2 ->
            val t3 = "NEVER_GONNA_HAPPEN"
            val iq = QueryLikelihood(listOf(t1, t2, t3))
            val gq = iq.toGalago(index.env)
            cmpResults("$t1, $t2, NULL", gq, iq, index)
        }
    }


    @Test
    fun testBM25EquivWithMissing() {
        val index = resource.index!!

        index.forEachTermPair { t1, t2 ->
            val t3 = "NEVER_GONNA_HAPPEN"
            val iq = UnigramRetrievalModel(listOf(t1, t2, t3), { BM25Expr(it) })
            val gq = iq.toGalago(index.env)
            cmpResults("$t1, $t2, NULL", gq, iq, index)
        }
    }

    @Test
    fun testEquivSDMMissing() {
        val index = resource.index!!

        index.forEachTermPair { t1, t2 ->
            val t3 = "NEVER_GONNA_HAPPEN"
            val iq = SequentialDependenceModel(listOf(t1, t2, t3))
            val gq2 = iq.toGalago(index.env)
            cmpResults("sdm($t1, $t2, NULL)", gq2, iq, index)

            // if this ever breaks, check to make sure defaults are in sync with Galago.
            val gq = GExpr("sdm").apply {
                addChild(GExpr.Text(t1))
                addChild(GExpr.Text(t2))
                addChild(GExpr.Text(t3))
            }
            cmpResults("sdm($t1, $t2, NULL)", gq, iq, index)
        }
    }

    @Test
    fun testEquivRRSDM() {
        val index = resource.index!!

        index.forEachTermPair { t1, t2 ->
            val t3 = "NEVER_GONNA_HAPPEN"
            val iq = SequentialDependenceModel(listOf(t1, t2, t3))
            // score everything no matter what:
            val gres = index.galago.transformAndExecuteQuery(iq.toGalago(index.env), pmake {
                set("annotate", true)
                set("requested", index.ND)
                set("working", index.names)
            })
            val gTruth = gres.scoredDocuments.associate { Pair(it.name, it) }

            val rrExpr = iq.toRRExpr(index.env)
            val rrScores = index.ltrIndex.associate { Pair(it.name, rrExpr.eval(it)) }

            Assert.assertEquals(rrScores.size, gTruth.size)
            index.names.forEach { name ->
                val sameDoc = gTruth[name]!!
                val expected = sameDoc.score
                val actual = rrScores[name]!!
                dblEquals(expected, actual) {
                    println("$name")
                    println(sameDoc.annotation)
                    println(rrExpr.explain(index.ltrIndex.find { it.name == name }!!))
                }
            }
        }
    }
}