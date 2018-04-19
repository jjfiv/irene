package edu.umass.cics.ciir.irene

import org.apache.lucene.index.PostingsEnum
import java.io.IOException

/**
 *
 * @author jfoley.
 */
typealias IOSystem = org.apache.lucene.store.Directory
typealias MemoryIO = org.apache.lucene.store.RAMDirectory
typealias DiskIO = org.apache.lucene.store.FSDirectory
typealias LDoc = org.apache.lucene.document.Document

class RefCountedIO(private val io: IOSystem) : java.io.Closeable
{
    var opened = 1
    override fun close() {
        synchronized(io) {
            if (--opened == 0) {
                io.close()
            }
        }
    }
    fun use(): IOSystem {
        assert(opened > 0)
        return io
    }
    fun open(): RefCountedIO {
        synchronized(io) {
            opened++
        }
        return this
    }
}

inline fun <T> lucene_try(action: ()->T): T? {
    return try {
        action()
    } catch (missing: IllegalArgumentException) {
        null
    } catch (ioe: IOException) {
        null
    }
}

enum class DataNeeded : Comparable<DataNeeded> {
    DOCS, COUNTS, POSITIONS, SCORES;
    fun textFlags(): Int = when(this) {
        DOCS -> PostingsEnum.NONE.toInt()
        COUNTS -> PostingsEnum.FREQS.toInt()
        POSITIONS -> PostingsEnum.ALL.toInt()
        SCORES -> error("Can't get scores from raw text.")
    }
}
