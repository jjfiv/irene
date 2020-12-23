package edu.umass.cics.ciir.irene.utils

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import java.io.*
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * Partial port of Galago's clever/simple StreamCreator class.
 * Thanks, Trevor Strohman.
 * @author jfoley.
 */
@Throws(IOException::class)
fun openInputStream(filename: String): InputStream {
    return if (filename.endsWith(".gz")) {
        GZIPInputStream(FileInputStream(filename))
    } else if (filename.endsWith(".bz") || filename.endsWith(".bz2")) {
        BZip2CompressorInputStream(FileInputStream(filename), true)
    } else {
        DataInputStream(FileInputStream(filename))
    }
}
fun openInputStream(file: File): InputStream = openInputStream(file.path)

@Throws(IOException::class)
fun openOutputStream(filename: String): OutputStream {
    File(filename).ensureParentDirectories()
    return if (filename.endsWith(".gz")) {
        GZIPOutputStream(FileOutputStream(filename))
    } else if (filename.endsWith(".bz") || filename.endsWith(".bz2")) {
        BZip2CompressorOutputStream(FileOutputStream(filename))
    } else {
        DataOutputStream(FileOutputStream(filename))
    }
}

fun openOutputStream(file: File) = openOutputStream(file.path)
