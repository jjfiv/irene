package edu.umass.cics.ciir.irene

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import edu.umass.cics.ciir.irene.indexing.IndexParams
import edu.umass.cics.ciir.irene.lang.QExprModule
import io.javalin.Javalin
import org.http4k.client.ApacheClient
import org.http4k.core.Method
import org.http4k.core.Request
import org.http4k.core.Response
import org.http4k.core.Status.Companion.OK
import org.junit.Assert.assertEquals
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.ExternalResource
import java.util.HashMap

import org.junit.Assert.assertNull


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

    private fun invokeGetAPI(path: String, params: Map<String, String>): Response {
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
        val r = invokeGetAPI("/api/indexes", mapOf())
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
}