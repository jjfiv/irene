package edu.umass.cics.ciir.irene

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.indexing.IndexParams
import edu.umass.cics.ciir.irene.lang.BM25Expr
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.QExprModule
import edu.umass.cics.ciir.irene.lang.TextExpr
import io.javalin.Javalin
import org.http4k.client.ApacheClient
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status.Companion.OK
import org.junit.Assert.*
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.ExternalResource
import java.util.*


class SharedIndex : ExternalResource() {
    var index: IreneIndex? = null
    var server: Javalin? = null
    val mapper: ObjectMapper = ObjectMapper()
        .registerKotlinModule()
        .registerModule(QExprModule())
    override fun before() {
        index = makeIndex()
        server = APIServer.run("localhost", 4444, mapOf("default" to index!!))
    }
    override fun after() {
        server?.stop()
        index?.close()
    }
}

private fun makeIndex(): IreneIndex {
    return IndexParams().apply {
        defaultField = "text"
        inMemory()
    }.openWriter().use { writer ->
        writer.doc {
            setId("A")
            setTextField("text", "a b c")
            setBoolField("canonical", true)
        }
        writer.doc {
            setId("B")
            setTextField("text", "b c d e")
            setBoolField("canonical", false)
        }
        writer.doc {
            setId("C")
            setTextField("text", "c d e f g")
            setBoolField("canonical", true)
        }
        writer.doc {
            setId("D")
            setTextField("text", "d e f g h i")
            setBoolField("canonical", true)
        }

        writer.commit()
        writer.open()
    }
}

class APITest {
    companion object {
        @ClassRule
        @JvmField
        val resource = SharedIndex()
    }

    private fun <T> invokePostAPI(path: String, params: T): Response {
        val client = ApacheClient()
        var request = Request(Method.POST, "http://localhost:4444${path}")
        request.header("content-type", "application/json")
        val bodyStr =resource.mapper.writeValueAsString(params)
        request = request.body(bodyStr)
        return client(request)
    }
    private fun invokeGetAPI(path: String, params: Map<String, String> = mapOf()): Response {
        val client = ApacheClient()
        var request = Request(Method.GET, "http://localhost:4444${path}")
        for ((k, v) in params) {
            request = request.query(k, v)
        }
        return client(request)
    }

    @Test
    fun testListIndexes() {
        val index = resource.index!!
        val mapper = resource.mapper
        val r = invokeGetAPI("/api/indexes")
        assertEquals(r.status, OK)
        val typeFactory = mapper.typeFactory
        val mapType = typeFactory.constructMapType(HashMap::class.java, String::class.java, IndexInfo::class.java)
        val map: Map<String, IndexInfo> = mapper.readValue(r.bodyString(), mapType)
        assertEquals(map.size, 1)
        val found = map["default"]!!
        assertEquals(found.idFieldName, index.idFieldName)
        assertEquals(found.defaultField, index.defaultField)
        assertNull(found.path)
    }

    @Test
    fun testGetDoc() {
        val index = resource.index!!
        val mapper = resource.mapper
        val r = invokeGetAPI("/api/doc/default", mapOf("id" to "A"))
        assertEquals(r.status, OK)
        val node = mapper.readTree(r.bodyString())
        print(r.bodyString())
        assertEquals("A", node[index.idFieldName].asText())
        assertEquals("a b c", node[index.defaultField].asText())
        assertEquals("T", node["canonical"].asText())
    }

    @Test
    fun testRandomDoc() {
        val index = resource.index!!
        val mapper = resource.mapper

        val found = hashSetOf<String>()
        val expected = hashSetOf("A", "B", "C", "D")
        var trials = 0
        while (found.size < index.reader.numDocs() && trials < 1000) {
            val r = invokeGetAPI("/api/random/default")
            assertEquals(r.status, OK)
            val dr = mapper.readValue(r.bodyString(), DocResponse::class.java)
            found.add(dr.name)
            assertTrue("${dr.name} not expected...", dr.name in expected)
            assertEquals(dr.score, 1.0f, 0.0001f)
            trials += 1
        }
        assertEquals("Could not find all documents from a set of ${expected.size} randomly in $trials trials!", expected, found)
    }

    @Test
    fun testIndexConfig() {
        val index = resource.index!!
        val mapper = resource.mapper
        val r = invokeGetAPI("/api/config/default")
        assertEquals(r.status, OK)
        val config = mapper.readValue(r.bodyString(), EnvConfig::class.java)
        assertEquals(index.defaultField, config.defaultField)
    }

    @Test
    fun testTokenizePost() {
        val mapper = resource.mapper
        val r = invokePostAPI("/api/tokenize", TokenizeRequest("Hello World!", "default"))
        assertEquals(r.status, OK)
        val resp = mapper.readValue(r.bodyString(), TokenizeResponse::class.java)
        assertEquals(listOf("hello", "world"), resp.terms)
    }

    @Test
    fun testPreparePost() {
        val index = resource.index!!
        val mapper = resource.mapper
        val q = BM25Expr(TextExpr("c"))
        val localPrep = index.env.prepare(q)
        val r = invokePostAPI("/api/prepare", PrepareRequest(q, "default"))
        assertEquals(r.status, OK)
        val remotePrep = mapper.readValue(r.bodyString(), QExpr::class.java)
        assertEquals(localPrep, remotePrep)
    }

    @Test
    fun testQueryPost() {
        val index = resource.index!!
        val mapper = resource.mapper
        val n = 4

        for (text in listOf("a", "b", "c", "d")) {
            val q = BM25Expr(TextExpr(text))
            val localResults = index.search(q, n)
            val r = invokePostAPI("/api/query", QueryRequest(q, "default", n))
            assertEquals(r.status, OK)
            val remoteResults = mapper.readValue(r.bodyString(), QueryResponse::class.java)
            assertEquals(localResults.totalHits, remoteResults.totalHits)
            assertEquals(localResults.scoreDocs.size, remoteResults.topdocs.size)
        }
    }

    @Test
    fun testDocSetPost() {
        val index = resource.index!!
        val mapper = resource.mapper
        val n = 4

        for (text in listOf("a", "b", "c", "d")) {
            val q = BM25Expr(TextExpr(text))
            val localResults = index.search(q, n)
            val names = localResults.scoreDocs.map { index.getDocumentName(it.doc) }.toSet()
            val r = invokePostAPI("/api/docset", QueryRequest(q, "default", n))
            assertEquals(r.status, OK)
            val remoteResults = mapper.readValue(r.bodyString(), SetResponse::class.java)
            assertEquals(localResults.totalHits, remoteResults.totalHits)
            assertEquals(names, remoteResults.matches.toSet())
        }
    }

    @Test
    fun testSamplePost() {
        val index = resource.index!!
        val mapper = resource.mapper
        val n = 2

        for (text in listOf("a", "b", "c", "d")) {
            val q = BM25Expr(TextExpr(text))

            // First; collection is deterministic on seed:
            val localResults = index.sample(q.copy(), n, Random(13L))
            val names = localResults.mapTo(hashSetOf()) { index.getDocumentName(it)!! }
            val localResults2 = index.sample(q.copy(), n, Random(13L))
            val names2 = localResults2.mapTo(hashSetOf()) { index.getDocumentName(it)!! }
            assertEquals(names, names2)

            // Second; remote and local are the same:
            val r = invokePostAPI("/api/sample", SampleRequest(q, "default", depth=n, seed=13L))
            assertEquals(r.status, OK)
            val remoteResults = mapper.readValue(r.bodyString(), SetResponse::class.java)
            assertEquals(localResults.totalOffered.toLong(), remoteResults.totalHits)
            assertEquals(names, remoteResults.matches.toSet())
        }
    }
}