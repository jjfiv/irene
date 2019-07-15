package edu.umass.cics.ciir.irene.trec2018.news

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.PredTruth
import edu.umass.cics.ciir.irene.RankingMeasures
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.LinearQLExpr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.QueryLikelihood
import edu.umass.cics.ciir.irene.lang.SequentialDependenceModel
import edu.umass.cics.ciir.irene.ltr.LTRDoc
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import edu.umass.cics.ciir.irene.trec2018.defaultWapoIndexPath
import edu.umass.cics.ciir.irene.utils.StreamingStats
import edu.umass.cics.ciir.irene.utils.computeEntropy
import edu.umass.cics.ciir.irene.utils.smartPrint
import edu.umass.cics.ciir.irene.utils.timed
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.time.ZoneId
import java.util.*

data class ScoredWapoDoc(val relevance: Int, val score: Double, val fields: WapoDocument, val features: HashMap<String, Double>)
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
    val judgments: Map<String, QueryJudgments> = QuerySetJudgments("$queryDir/newsir18-background-linking.qrel")

    val numTerms = argp.get("numTerms", 50)
    val lambda = argp.get("smoothLambda", 0.8)

    val indexPath = File(argp.get("index", defaultWapoIndexPath))
    if (!indexPath.exists() || !indexPath.isDirectory) {
        throw IllegalArgumentException("index is not valid: $indexPath")
    }

    println(queries.size)
    println(judgments.size)

    val stopwords = inqueryStop
    val mAP = StreamingStats()

    val LA_Zone = ZoneId.of("America/Los_Angeles")

    File("trec_news.ltr.jsonl.gz").smartPrint { output ->
        IreneIndex(IndexParams().apply { withPath(indexPath) }).use { index ->
            index.env.defaultDirichletMu = index.getAverageDL(index.defaultField)
            index.env.optimizeDirLog = false
            index.env.optimizeBM25 = true
            println("Total Documents: ${index.totalDocuments}")
            val targetField = index.defaultField

            for (q in queries) {
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
                val expansionExpr = rm.toQExpr(numTerms, scorer = { LinearQLExpr(it, lambda) })

                val (scoringTime, results) = timed { index.search(expansionExpr, BackgroundLinkingDepth * 3) }
                println("Scoring time: $scoringTime")

                var selfMRR = 0.0;

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

                    val position = titleDedup.indexOfFirst { it.doc == docNo }
                    selfMRR = if (position < 0) {
                        0.0
                    } else {
                        1.0 / (position + 1)
                    }
                    val recommendations = titleDedup.filter { it.doc != docNo }
                    println("Self-MRR: $selfMRR, ${recommendations.size}")
                    recommendations
                }
                println("Dedup time: ${dedupTime}")

                val labels = judgments[q.qid] ?: error("No judgments for q=${q.qid}")
                println("Labels: ${labels.relevantJudgmentCount}/${labels.documentSet.size}")


                val scoredDocs = dedupResults.map {
                    val ldoc = index.document(it.doc) ?: error("Missing document: ${it.doc} for query=${q.qid}")
                    val wdoc = fromLucene(index, ldoc)
                    ScoredWapoDoc(labels.get(wdoc.id), it.score.toDouble(), wdoc, hashMapOf())
                }

                val ltrDocs = scoredDocs.associate { it.fields.id to LTRDoc(it.fields.id, it.fields.body, index.defaultField, index.tokenizer) }
                val scoredDocsById = scoredDocs.associate { it.fields.id to it }

                val featureFns = hashMapOf<String, QExpr>()

                featureFns["rm-n10"] = rm.toQExpr(10)
                featureFns["rm-n50"] = rm.toQExpr(50)
                featureFns["rm-n100"] = rm.toQExpr(100)

                val q_paragraphs = doc.body.split("\n\n")

                featureFns["title-ql"] = QueryLikelihood(index.tokenize(doc.title ?: q_paragraphs[0]))
                featureFns["first-para-ql"] = QueryLikelihood(index.tokenize(q_paragraphs[0]))
                featureFns["title-sdm"] = SequentialDependenceModel(index.tokenize(doc.title ?: q_paragraphs[0]))
                featureFns["first-para-sdm"] = SequentialDependenceModel(index.tokenize(q_paragraphs[0]))

                val ltrEnv = index.getRREnv()

                for ((docid, ltrDoc) in ltrDocs) {
                    val sdoc = scoredDocsById[docid]!!
                    for ((name, expr) in featureFns) {
                        sdoc.features[name] = ltrDoc.eval(ltrEnv, expr)
                    }
                    val terms = ltrDoc.fields[index.defaultField]!!.terms
                    sdoc.features["entropy"] = terms.computeEntropy()
                    sdoc.features["num_para"] = sdoc.fields.body.split("\n\n").size.toDouble()
                    sdoc.features["num_terms"] = terms.size.toDouble()
                    sdoc.features["q-self-mrr"] = selfMRR
                }

                val qPublishDate = doc.published_date ?: 0L;
                val queryInstant = Date(qPublishDate).toInstant().atZone(LA_Zone)

                fun bool(truth: Boolean): Double = if (truth) { 1.0 } else { -1.0 }

                for (sdoc in scoredDocs) {
                    val publishDate = sdoc.fields.published_date ?: 0L;
                    val docInstant = Date(publishDate).toInstant().atZone(LA_Zone)
                    if (sdoc.fields.published_date != null) {
                        sdoc.features["date-cmp"] = publishDate.compareTo(qPublishDate).toDouble()
                        sdoc.features["date-sub"] = (qPublishDate - publishDate).toDouble()
                        sdoc.features["same-day-of-week"] = bool(queryInstant.dayOfWeek == docInstant.dayOfWeek)
                        sdoc.features["same-month"] = bool(queryInstant.month == docInstant.month)
                        sdoc.features["same-year"] = bool(queryInstant.year == docInstant.year)
                        sdoc.features["pub-${docInstant.dayOfWeek}"] = 1.0
                    }
                }

                val evalList = scoredDocs.map { PredTruth(it.relevance > 0, it.score) }
                val AP = RankingMeasures.computeAP(evalList, numRelevant = labels.relevantJudgmentCount)
                println("AP=$AP")
                mAP.push(AP)

                output.println( mapper.writeValueAsString(QueryExtract(q, doc, scoredDocs)) )
            } // for q in queries

            println("AP=${mAP}")
        } // with index
    } // smartPrint

}