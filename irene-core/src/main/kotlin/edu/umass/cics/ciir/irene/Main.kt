package edu.umass.cics.ciir.irene

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.indexing.IndexParams
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.QExprModule
import edu.umass.cics.ciir.irene.utils.ReservoirSampler
import io.javalin.Javalin
import io.javalin.plugin.json.JavalinJackson
import org.roaringbitmap.RoaringBitmap
import java.io.File
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import kotlin.collections.HashMap

val mapper: ObjectMapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(QExprModule())

data class TokenizeRequest(val text: String, val index: String, val field: String? = null)
data class TokenizeResponse(val terms: List<String>)
data class DocResponse(val name: String, val score: Float, var document: Map<String, Any>? = null)
data class QueryResponse(val topdocs: List<DocResponse>, val totalHits: Long)
data class SetResponse(val matches: List<String>, val totalHits: Long)
data class PrepareRequest(val query: QExpr, val index: String)
data class SampleRequest(val query: QExpr, val index: String, val count: Int, val seed: Long? = null)
data class QueryRequest(val query: QExpr, val index: String, val depth: Int, val offset: Int = 0, val getDocuments: Boolean = false)
data class IndexInfo(val idFieldName: String, val path: String, val defaultField: String)
data class IndexSpec(val name: String, val path: String, val idFieldName: String?, val defaultField: String?)
data class ConfigFileContents(val indexes: List<IndexSpec>)

object APIServer {
    fun run(host: String, port: Int, indexes: Map<String, IreneIndex>): Javalin {
        JavalinJackson.configure(mapper)
        val app = Javalin.create().start(host, port)
        println("launch $host:$port")

        /// Print Exceptions so that debugging is possible / not annoying.
        app.exception(Exception::class.java) { e, _ ->
            e.printStackTrace(System.err)
        }

        app.get("/api/status") { ctx ->
            ctx.json(mapOf("status" to "OK"))
        }

        app.get("/api/doc/:index") { ctx ->
            val indexId = ctx.pathParam("index")
            val index = indexes[indexId] ?: error("Must open '$indexId' before using it.")
            val id = ctx.queryParam("id") ?: error("Must specify a document id.")
            val internal = index.documentById(id) ?: error("No such document: $id")
            ctx.json(index.docAsMap(internal)!!)
        }

        app.get("/api/random/:index") { ctx ->
            val indexId = ctx.pathParam("index")
            val index = indexes[indexId] ?: error("Must open '$indexId' before using it.")
            for (_try in 0..30) {
                val id = ThreadLocalRandom.current().nextInt(0, index.totalDocuments)
                val doc = index.document(id, setOf(index.idFieldName)) ?: continue
                // Skip blank pages.
                val docId = doc.getField(index.idFieldName)?.stringValue() ?: continue
                ctx.json(DocResponse(docId, 1.0f))
                return@get
            }
            ctx.status(501)
            ctx.result("Unable to find non-empty random document.")
        }

        app.get("/api/indexes") { ctx ->
            ctx.json(indexes.mapValues { (_, index) ->
                IndexInfo(index.idFieldName, index.params.filePath.toString(), index.defaultField)
            })
        }

        app.get("/api/config/:index") { ctx ->
            val indexId = ctx.pathParam("index")
            val index = indexes[indexId] ?: error("Must open '$indexId' before using it.")
            ctx.json(index.env.config)
        }

        app.get("/api/tokenize/:index") { ctx ->
            val indexId = ctx.pathParam("index")
            val index = indexes[indexId] ?: error("Must open '$indexId' before using it.")
            val text = ctx.queryParam("text") ?: error("'text' is required.")
            val field = ctx.queryParam("field") ?: index.env.defaultField
            val terms = index.tokenize(text, field)
            ctx.json(TokenizeResponse(terms))
        }

        app.post("/api/tokenize") { ctx ->
            val req = ctx.bodyValidator<TokenizeRequest>().get()
            val index = indexes[req.index] ?: error("Must open '${req.index}' before using it.")
            val text = req.text
            val field = req.field ?: index.env.defaultField
            val terms = index.tokenize(text, field)
            ctx.json(TokenizeResponse(terms))
        }

        app.post("/api/prepare") {ctx ->
            System.err.println("BODY-PREPARE " + ctx.body())
            val req = ctx.bodyValidator<PrepareRequest>().get()
            val index = indexes[req.index] ?: error("no such index ${req.index}")
            ctx.json(index.env.prepare(req.query))
        }

        app.post("/api/sample") {ctx ->
            val req = ctx.bodyValidator<SampleRequest>().get()
            val index = indexes[req.index] ?: error("no such index ${req.index}")
            val seed = req.seed ?: ThreadLocalRandom.current().nextLong()
            val results: ReservoirSampler<Int> = index.sample(req.query, req.count, Random(seed))
            val docs = results.map { index.getDocumentName(it)!! }
            ctx.json(SetResponse(docs, results.totalOffered.toLong()))
        }

        app.post("/api/docset") { ctx ->
            val req = ctx.bodyValidator<QueryRequest>().get()
            val index = indexes[req.index] ?: error("no such index ${req.index}")
            val results: RoaringBitmap = index.matches(req.query)
            val docs = results.map { index.getDocumentName(it)!! }
            ctx.json(SetResponse(docs, results.longCardinality))
        }

        app.post("/api/query") { ctx ->
            val req = ctx.bodyValidator<QueryRequest>().get()
            val index = indexes[req.index] ?: error("no such index ${req.index}")
            val results = index.search(req.query, req.depth + req.offset)
            val start = req.offset
            val end = (req.offset + req.depth).coerceAtMost(results.scoreDocs.size)
            val docs = results.scoreDocs.slice(start until end).map { sdoc ->
                var document: Map<String, Any>? = null
                if (req.getDocuments) {
                    document = index.docAsMap(sdoc.doc)
                        ?: error("Index corrupted; document=${sdoc.doc} retrieved but not present.")
                }
                DocResponse(index.getDocumentName(sdoc.doc)!!, sdoc.score, document)
            }
            ctx.json(QueryResponse(docs, results.totalHits))
        }

        return app
    }
}

fun main(args: Array<String>) {
    var host = "localhost"
    var port = 4444
    val indexes = HashMap<String, IreneIndex>()

    var i = 0
    while (i < args.size) {
        when(args[i]) {
            "--host" -> {
                host = args[i+1]
                i += 2
            }
            "--port" -> {
                port = args[i+1].toInt()
                i += 2
            }
            "--config-file-path", "--config" -> {
                val configFilePath = args[i+1]
                val contents = mapper.readValue(File(configFilePath), ConfigFileContents::class.java)
                for (info in contents.indexes) {
                    val index = IndexParams().apply {
                        withPath(File(info.path))
                        if (info.defaultField != null) {
                            defaultField = info.defaultField
                        }
                        if (info.idFieldName != null) {
                            idFieldName = info.idFieldName
                        }
                    }.openReader()

                    indexes[info.name] = index
                }
                i += 2
            }
            else -> {
                error("Unexpected argument: ${args[i]}; try --host STR, --port INT, or --config FILENAME")
            }
        }
    }

    APIServer.run(host, port, indexes)

}