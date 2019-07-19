package edu.umass.cics.ciir.irene.trec2018.news

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.BM25Expr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.QueryLikelihood
import edu.umass.cics.ciir.irene.lang.SequentialDependenceModel
import edu.umass.cics.ciir.irene.ltr.LTRDoc
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import edu.umass.cics.ciir.irene.trec2018.defaultWapoIndexPath
import edu.umass.cics.ciir.irene.utils.computeEntropy
import edu.umass.cics.ciir.irene.utils.smartPrint
import edu.umass.cics.ciir.irene.utils.timed
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.*

data class ScoredWapoDoc(val relevance: Int, val fields: WapoDocument, val features: HashMap<String, Double>)
data class QueryExtract(val query: TrecNewsBGLinkQuery, val doc: WapoDocument, val candidates: List<ScoredWapoDoc>)

/**
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {
    val mapper = ObjectMapper().registerKotlinModule()
    val argp = Parameters.parseArgs(args)
    val homeDir = File(System.getenv("HOME"))
    val queryDir = File(argp.get("queryDir", "$homeDir/code/queries/trec_news"))
    val queries = loadBGLinkQueries("$queryDir/newsir18-background-linking-topics.v2.xml")
    val judgments: Map<String, QueryJudgments> = QuerySetJudgments("$queryDir/newsir18-background-linking.qrel", false, false)

    val numTerms = argp.get("numTerms", 50)

    val indexPath = File(argp.get("index", defaultWapoIndexPath))
    if (!indexPath.exists() || !indexPath.isDirectory) {
        throw IllegalArgumentException("index is not valid: $indexPath")
    }

    println(queries.size)
    println(judgments.size)

    val stopwords = inqueryStop

    File("trec_news.ltr.v2.jsonl.gz").smartPrint { output ->
        IreneIndex(IndexParams().apply { withPath(indexPath) }).use { index ->
            index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)
            index.env.optimizeDirLog = false
            index.env.optimizeBM25 = true
            index.env.estimateStats = "min"

            println("Total Documents: ${index.totalDocuments}")
            val targetField = index.defaultField

            fun poolQueryExpr(expr: QExpr, docNo: Int): List<Int> {
                val (scoringTime, results) = timed { index.search(expr, BackgroundLinkingDepth * 3) }

                val (dedupTime, dedupResults) = timed {
                    val titles = hashMapOf<Int, String>()
                    val kickers = hashMapOf<Int, String>()
                    for (sd in results.scoreDocs) {
                        index.getField(sd.doc, "title")?.stringValue()?.let {
                            titles.put(sd.doc, it)
                        }
                        index.getField(sd.doc, "kicker")?.stringValue()?.let {
                            kickers.put(sd.doc, it)
                        }
                    }
                    val titleDedup = results.scoreDocs
                            // reject duplicates:
                            .filter {
                                val title = titles.get(it.doc)
                                // hard-feature: has-title and most recent version:
                                title != null && title != "null" && title != titles.get(it.doc + 1)
                            }
                            .filterNot {
                                // Reject opinion/editorial.
                                skipTheseKickers.contains(kickers[it.doc] ?: "null")
                            }
                    titleDedup.filter { it.doc != docNo }
                }
                println("Scoring-time: $scoringTime dedup-time: ${dedupTime}")

                return dedupResults.map { it.doc }
            }

            for (q in queries) {
                println(q.qid)
                val docNo = index.documentById(q.docid) ?: error("Missing document for ${q.qid}")
                val lDoc = index.document(docNo) ?: error("Missing document fields for ${q.qid} internally: $docNo")
                val doc = fromLucene(index, lDoc)
                val tokens = index.tokenize(doc.body)

                val counts = TObjectIntHashMap<String>()
                val length = tokens.size.toDouble()
                for (t in tokens) {
                    if (stopwords.contains(t)) continue
                    counts.adjustOrPutValue(t, 1, 1)
                }
                val weights = TObjectDoubleHashMap<String>()
                counts.forEachEntry { term, count ->
                    weights.put(term, count / length)
                    true
                }
                val rm = RelevanceModel(weights, targetField)
                val q_paragraphs = doc.body.split("\n\n")
                val titleSDM = SequentialDependenceModel(index.tokenize(doc.title ?: q_paragraphs[0]), stopwords=stopwords)

                val pool = hashSetOf<Int>()
                pool.addAll(poolQueryExpr(rm.toQExpr(10, scorer = { BM25Expr(it) }), docNo))
                pool.addAll(poolQueryExpr(rm.toQExpr(50, scorer = { BM25Expr(it) }), docNo))
                pool.addAll(poolQueryExpr(rm.toQExpr(100, scorer = { BM25Expr(it) }), docNo))
                pool.addAll(poolQueryExpr(titleSDM, docNo))

                val labels = judgments[q.qid] ?: error("No judgments for q=${q.qid}")

                val scoredDocs = pool.map {
                    val ldoc = index.document(it) ?: error("Missing document: ${it} for query=${q.qid}")
                    val wdoc = fromLucene(index, ldoc)
                    ScoredWapoDoc(labels.get(wdoc.id), wdoc, hashMapOf())
                }


                val ltrDocs = scoredDocs.associate { it.fields.id to LTRDoc(it.fields.id, it.fields.body, index.defaultField, index.tokenizer) }
                val scoredDocsById = scoredDocs.associateBy { it.fields.id }

                val featureFns = hashMapOf<String, QExpr>()

                featureFns["bm25-rm-n10"] = rm.toQExpr(10, scorer={BM25Expr(it)})
                featureFns["bm25-rm-n50"] = rm.toQExpr(50, scorer={BM25Expr(it)})
                featureFns["bm25-rm-n100"] = rm.toQExpr(100, scorer={BM25Expr(it)})

                featureFns["rm-n10"] = rm.toQExpr(10)
                featureFns["rm-n50"] = rm.toQExpr(50)
                featureFns["rm-n100"] = rm.toQExpr(100)
                //featureFns["lce-n10"] = rm.toQExpr(10, discountStatsEnv=index.env)
                //featureFns["lce-n50"] = rm.toQExpr(50, discountStatsEnv=index.env)
                //featureFns["lce-n100"] = rm.toQExpr(100, discountStatsEnv=index.env)

                featureFns["title-ql"] = QueryLikelihood(index.tokenize(doc.title ?: q_paragraphs[0]))
                featureFns["first-para-ql"] = QueryLikelihood(index.tokenize(q_paragraphs[0]))
                featureFns["body-ql"] = QueryLikelihood(index.tokenize(doc.body))

                featureFns["title-sdm"] = titleSDM
                featureFns["first-para-sdm"] = SequentialDependenceModel(index.tokenize(q_paragraphs[0]), stopwords=stopwords)

                val ltrEnv = index.getRREnv()

                for ((docid, ltrDoc) in ltrDocs) {
                    val sdoc = scoredDocsById[docid]!!
                    for ((name, expr) in featureFns) {
                        sdoc.features[name] = ltrDoc.eval(ltrEnv, expr)
                    }
                    val terms = ltrDoc.fields[index.defaultField]!!.terms
                    sdoc.features["entropy"] = terms.computeEntropy()
                }

                val qPublishDate = doc.published_date ?: 0L;

                for (sdoc in scoredDocs) {
                    val publishDate = sdoc.fields.published_date ?: 0L;
                    if (sdoc.fields.published_date != null) {
                        sdoc.features["date-cmp"] = publishDate.compareTo(qPublishDate).toDouble()
                        sdoc.features["date-sub"] = (qPublishDate - publishDate).toDouble()
                    }
                }

                output.println( mapper.writeValueAsString(QueryExtract(q, doc, scoredDocs)) )
            } // for q in queries

        } // with index
    } // smartPrint

}