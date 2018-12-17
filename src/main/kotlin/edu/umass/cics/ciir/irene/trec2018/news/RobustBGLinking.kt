package edu.umass.cics.ciir.irene.trec2018.news

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.LuceneTokenizer
import edu.umass.cics.ciir.irene.galago.NamedMeasures
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.toRRExpr
import edu.umass.cics.ciir.irene.ltr.LTRDoc
import edu.umass.cics.ciir.irene.ltr.LTRDocField
import edu.umass.cics.ciir.irene.utils.*
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.core.eval.SimpleEvalDoc
import org.lemurproject.galago.core.eval.metric.QueryEvaluatorFactory
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.lists.Ranked
import java.io.File
import java.util.*

data class TrainingInfo(val qid: String, val tokens: List<String>, val judgments: QueryJudgments, val pool: List<LTRDoc>) { }

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args);
    val qrels = QuerySetJudgments.loadJudgments(argp.get("qrel",
            "/Users/jfoley/code/queries/robust04/robust04.qrels"),
            false, true)
    val params = IndexParams().withPath(File(argp.get("index", "robust04.irene")))

    val splits = hashMapOf<Int,MutableList<String>>()
    qrels.keys.toList().shuffled().forEachIndexed { i,qid ->
        splits.computeIfAbsent(i%5, {ArrayList()}).add(qid)
    }

    // picked by intuition
    val weight = 0.1
    val numTerms = 100

    val ndcg = QueryEvaluatorFactory.create("ndcg", Parameters.create());
    val ndcg5 = QueryEvaluatorFactory.create("ndcg5", Parameters.create());

    val rand = Random(42)
    IreneIndex(params).use {index ->
        index.env.defaultDirichletMu = 4000.0
        val trainingSet = qrels.entries.sample(argp.get("train", 30), rand)
        val testSet = qrels.entries.filter { it !in trainingSet }

        val msg = CountingDebouncer(trainingSet.size.toLong())
        val queryToDocNo: Map<String, TrainingInfo> = trainingSet.associate { (qid, judgments) ->
            val keep: List<Int> = judgments.entries.flatMap { (doc, label) ->
                if (label > 0) {
                    index.documentById(doc)?.let {
                        return@flatMap listOf(it)
                    }
                }
                emptyList<Int>()
            }
            val docNo = keep.sample(1, rand)[0]
            val tokens = index.terms(docNo)

            val query = documentVectorToQuery(tokens, numTerms, targetField=index.defaultField, unigramWeight=weight)
            val pool: List<LTRDoc> = index.search(query, 200)
                    .scoreDocs.filter { it.doc != docNo }
                    .map {
                        val doc = index.document(it.doc, setOf(index.idFieldName, index.defaultField))!!
                        val id = doc.getField(index.idFieldName).stringValue()
                        val body = LTRDocField(index.defaultField, doc.getField(index.defaultField).stringValue(), LuceneTokenizer(index.analyzer))
                        LTRDoc(id, hashMapOf(), hashMapOf(body.toEntry()), index.defaultField)
                    }
            msg.incr()?.let {
                println("Pool prepared for $qid... $it")
            }
            Pair(qid, TrainingInfo(qid, tokens, judgments, pool))
        }

        for (mu in listOf(3000,3500,4000)) {
            for (lambda in listOf(1)) {
                for (nt in listOf(10,20,50,100)) {
                    for (wds in listOf(8)) {
                        val msrs = NamedMeasures()
                        val (ltr_timing) = timed {
                            queryToDocNo.forEach { (qid, info) ->
                                val query = documentVectorToQuery(info.tokens, nt,
                                        targetField = index.defaultField,
                                        unigramWeight = lambda / 10.0,
                                        proxToExpr = { SmallerCountExpr(it) })
                                //index.env.defaultDirichletMu = mu.toDouble()
                                val expr = query.toRRExpr(index.env)

                                val scored = info.pool.mapTo(ArrayList()) {
                                    SimpleEvalDoc(it.name, -1, expr.eval(it))
                                }
                                Ranked.setRanksByScore(scored)
                                msrs.push("ndcg", ndcg.evaluate(QueryResults(qid, scored), info.judgments))
                                msrs.push("ndcg5", ndcg5.evaluate(QueryResults(qid, scored), info.judgments))
                            }
                        }
                        println(String.format("$mu\t$lambda\t$nt\t$wds\t%f\t%f\t%1.3f", msrs["ndcg"].mean, msrs["ndcg5"].mean, ltr_timing))
                    }
                }
            }
        }
    }

}
