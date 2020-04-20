package edu.umass.cics.ciir.irene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.index.IndexWriterConfig
import java.io.File
import java.util.HashMap

/**
 *
 * @author jfoley.
 */
class IndexParams {
    var defaultField = "body"
    var defaultAnalyzer: Analyzer = IreneEnglishAnalyzer()
    private var perFieldAnalyzers = HashMap<String, Analyzer>()
    var directory: RefCountedIO? = null
    var openMode: IndexWriterConfig.OpenMode? = null
    var idFieldName = "id"
    var filePath: File? = null

    fun withAnalyzer(field: String, analyzer: Analyzer): IndexParams {
        perFieldAnalyzers[field] = analyzer
        return this
    }
    fun inMemory(): IndexParams {
        directory = RefCountedIO(MemoryIO())
        create()
        return this
    }
    fun withPath(fp: File): IndexParams {
        filePath = fp
        directory = RefCountedIO(DiskIO.open(fp.toPath()))
        return this
    }
    fun create(): IndexParams {
        openMode = IndexWriterConfig.OpenMode.CREATE
        return this
    }
    fun append(): IndexParams {
        openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND
        return this
    }
    val analyzer: Analyzer
        get() = if (perFieldAnalyzers.isEmpty()) {
            defaultAnalyzer
        } else {
            PerFieldAnalyzerWrapper(defaultAnalyzer, perFieldAnalyzers)
        }

    //fun openReader() = IreneIndex(this)
    //fun openWriter() = IreneIndexer(this)
}
