package edu.umass.cics.ciir.irene

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.expr_from_json
import org.lemurproject.galago.tupleflow.web.WebHandler
import org.lemurproject.galago.tupleflow.web.WebServer
import org.lemurproject.galago.utility.Parameters
import java.io.File
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

val mapper = ObjectMapper().registerKotlinModule()

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

class IreneAPIServer(val argp: Parameters) : WebHandler {
    val indexes = HashMap<String, IreneIndex>()
    override fun handle(request: HttpServletRequest, response: HttpServletResponse) {
        try {
            val GET = request.method == "GET"
            val POST = request.method == "POST"
            when (request.pathInfo) {
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
                    response.sendRawJSON(index.docAsParameters(internal).toString())
                }
                "/query" -> if (POST && request.contentType == "application/json") {
                    val qp = Parameters.parseReader(request.reader)
                    // data class QueryRequest(val index: String, val depth: Int, val query: QExpr)
                    val depth = qp.getInt("depth")
                    val index_id = qp.getString("index") ?: error("must provide index")
                    val index = this.indexes[index_id] ?: error("no such index=${index_id}")
                    val query = expr_from_json(qp.getMap("query"))
                    val results = index.search(query, depth)
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
        argp.put("port", 1234)
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
