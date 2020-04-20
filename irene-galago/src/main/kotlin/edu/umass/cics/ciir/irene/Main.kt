package edu.umass.cics.ciir.irene

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.galago.pmake
import edu.umass.cics.ciir.irene.indexing.IndexParams
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.QExprModule
import org.lemurproject.galago.tupleflow.web.WebHandler
import org.lemurproject.galago.tupleflow.web.WebServer
import org.lemurproject.galago.utility.Parameters
import java.io.File
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

val mapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(QExprModule())

fun HttpServletRequest.getParamOrNull(param: String): String? {
    val values = this.getParameterValues(param) ?: return null
    if (values.size != 1) {
        error("Received multiple parameters for $param.")
    }
    return values[0]
}

fun <T> HttpServletResponse.sendJSON(item: T) {
    contentType = "application/json"
    status = 200
    writer.use { out ->
        mapper.writeValue(out, item)
    }
}
fun HttpServletResponse.sendRawJSON(str: String) {
    contentType = "application/json"
    status = 200
    writer.use { out ->
        out.println(str)
    }
}

data class TokenizeResponse(val terms: List<String>)
data class DocResponse(val name: String, val score: Float)
data class QueryResponse(val topdocs: List<DocResponse>, val totalHits: Long)
data class PrepareRequest(val query: QExpr, val index: String)
data class QueryRequest(val query: QExpr, val index: String, val depth: Int)

class IreneAPIServer(val argp: Parameters) : WebHandler {
    val indexes = HashMap<String, IreneIndex>()
    override fun handle(request: HttpServletRequest, response: HttpServletResponse) {
        try {
            val GET = request.method == "GET"
            val POST = request.method == "POST"
            when (request.pathInfo) {
                "/indexes" -> if (GET) {
                    response.sendRawJSON(Parameters.wrap(indexes.mapValues { index ->
                        val params = index.value.params
                        pmake {
                            set("defaultField", params.defaultField)
                            set("path", params.filePath.toString())
                            set("idFieldName", params.idFieldName)
                        }
                    }).toString())
                }
                "/config" -> if (GET) {
                    val index = indexes[request.getParamOrNull("index") ?: error("index must be specified")]
                            ?: error("no such index!")
                    response.sendJSON(index.env.config)
                } else if (POST && request.contentType == "application/json") {
                    val index = indexes[request.getParamOrNull("index") ?: error("index must be specified")]
                            ?: error("no such index!")
                    // TODO: make this more fine-grained
                    index.env.config = mapper.readValue(request.reader, EnvConfig::class.java)
                }
                "/open" -> if (POST) {
                    val name = request.getParamOrNull("name") ?: error("Name for index must be assigned!")
                    if (indexes.contains(name)) {
                        response.sendError(400, "Already Open")
                        return
                    }
                    val path = request.getParamOrNull("path") ?: error("Path must be given!")
                    val params = IndexParams().apply {
                        withPath(File(path))
                    }
                    indexes[name] = params.openReader()
                }
                "/tokenize" -> if (GET) {
                    val index = indexes[request.getParamOrNull("index") ?: error("index must be specified")]
                            ?: error("no such index!")
                    val text = request.getParamOrNull("text") ?: error("text must be specified")
                    val field = request.getParamOrNull("field") ?: index.env.defaultField
                    val terms = index.tokenize(text, field)
                    response.sendJSON(TokenizeResponse(terms))
                }
                "/doc" -> if (GET) {
                    val index = indexes[request.getParamOrNull("index") ?: error("index must be specified")]
                            ?: error("no such index!")
                    val id = request.getParamOrNull("id") ?: error("doc id must be specified")
                    val internal = index.documentById(id) ?: error("No such id!")
                    response.sendJSON(index.docAsMap(internal))
                }
                "/prepare" -> if (POST && request.contentType == "application/json") {
                    val req = mapper.readValue(request.reader, PrepareRequest::class.java)
                    val index = this.indexes[req.index] ?: error("no such index=${req.index}")
                    response.sendJSON(index.env.prepare(req.query))
                }
                "/query" -> if (POST && request.contentType == "application/json") {
                    val req = mapper.readValue(request.reader, QueryRequest::class.java)
                    val index = this.indexes[req.index] ?: error("no such index=${req.index}")
                    val results = index.search(req.query, req.depth)
                    val docs = results.scoreDocs.map { sdoc ->
                        DocResponse(index.getDocumentName(sdoc.doc)!!, sdoc.score)
                    }
                    response.sendJSON(QueryResponse(docs, results.totalHits))
                }
            }
        } catch (e: Throwable) {
            e.printStackTrace(System.err)
            if (!response.isCommitted) {
                response.sendError(501, e.message)
            }
        }
    }

}

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
    val handler = IreneAPIServer(argp)
    val server = WebServer.start(argp, handler)
    println("running ${server.url}")
    server.join()
    println("shutdown")
}
