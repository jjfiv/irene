package edu.umass.cics.ciir.irene

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.indexing.IndexParams
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.QExprModule
import io.javalin.Javalin
import io.javalin.http.BadRequestResponse
import io.javalin.plugin.json.JavalinJackson
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.concurrent.ConcurrentHashMap

val mapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(QExprModule())

data class TokenizeResponse(val terms: List<String>)
data class DocResponse(val name: String, val score: Float)
data class QueryResponse(val topdocs: List<DocResponse>, val totalHits: Long)
data class PrepareRequest(val query: QExpr, val index: String)
data class QueryRequest(val query: QExpr, val index: String, val depth: Int)
data class IndexInfo(val idFieldName: String, val path: String, val defaultField: String)

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    if (!argp.containsKey("port")) {
        argp.put("port", 4444)
    }
    if (!argp.containsKey("host")) {
        argp.put("host", "localhost")
    }
    val host = argp.getStr("host")
    val port = argp.getInt("port")
    println("launch $host:$port")

    val indexes = ConcurrentHashMap<String, IreneIndex>()
    JavalinJackson.configure(mapper)
    val app = Javalin.create().start(host, port)

    app.get("/doc") {ctx ->
        val index_id = ctx.queryParam("index") ?: error("index must be specified")
        val index = indexes[index_id] ?: error("Must open '$index_id' before using it.")
        val id = ctx.queryParam("id") ?: error("Must specify a document id.")
        val internal = index.documentById(id) ?: error("No such document: $id")
        ctx.json(index.docAsMap(internal)!!)
    }

    app.get("/indexes") { ctx ->
        ctx.json(indexes.mapValues { (_, index) ->
            IndexInfo(index.idFieldName, index.params.filePath.toString(), index.defaultField)
        })
    }

    app.get("/config") { ctx ->
        val index_id = ctx.queryParam("index") ?: error("index must be specified")
        val index = indexes[index_id] ?: error("Must open '$index_id' before using it.")
        ctx.json(index.env.config)
    }

    app.post("/open") { ctx ->
        val name = ctx.formParam("name") ?: error("'name' is required.")
        val path = ctx.formParam("path") ?: error("'path' is required.")
        if (indexes.contains(name)) {
            throw BadRequestResponse("Already Open")
        } else {
            val params = IndexParams().apply {
                withPath(File(path))
            }
            indexes[name] = params.openReader()
        }
    }

    app.get("/tokenize") { ctx ->
        val index_id = ctx.queryParam("index") ?: error("index must be specified")
        val index = indexes[index_id] ?: error("Must open '$index_id' before using it.")
        val text = ctx.queryParam("text") ?: error("'text' is required.")
        val field = ctx.queryParam("text") ?: index.env.defaultField
        val terms = index.tokenize(text, field)
        ctx.json(TokenizeResponse(terms))
    }

    app.post("/prepare") {ctx ->
        val req = ctx.bodyValidator<PrepareRequest>().get()
        val index = indexes[req.index] ?: error("no such index ${req.index}")
        ctx.json(index.env.prepare(req.query))
    }

    app.post("/query") { ctx ->
        val req = ctx.bodyValidator<QueryRequest>().get()
        val index = indexes[req.index] ?: error("no such index ${req.index}")
        val results = index.search(req.query, req.depth)
        val docs = results.scoreDocs.map { sdoc ->
            DocResponse(index.getDocumentName(sdoc.doc)!!, sdoc.score)
        }
        ctx.json(QueryResponse(docs, results.totalHits))
    }
}
