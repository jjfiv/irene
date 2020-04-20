package edu.umass.cics.ciir.irene.indexing

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.IndexableField
import java.io.Closeable
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author jfoley.
 */
class IreneIndexer(val params: IndexParams) : Closeable {
    companion object {
        fun build(setup: IndexParams.() -> Unit): IreneIndexer {
            return IreneIndexer(IndexParams().apply(setup))
        }
    }
    val processed = AtomicLong(0)
    private val dest = params.directory!!
    val writer = IndexWriter(dest.use(), IndexWriterConfig(params.analyzer).apply {
        similarity = TrueLengthNorm()
        openMode = params.openMode
    })
    override fun close() {
        writer.close()
    }
    fun commit() {
        writer.commit()
    }
    fun push(vararg doc: IndexableField): Long {
        writer.addDocument(doc.toList())
        return processed.incrementAndGet()
    }
    fun push(doc: Iterable<IndexableField>): Long {
        writer.addDocument(doc)
        return processed.incrementAndGet()
    }
    fun open() = IreneIndex(dest, params)

    fun doc(fn: LDocBuilder.()->Unit): Long {
        val doc = LDocBuilder(params)
        fn(doc)
        return push(doc.finish())
    }
}

