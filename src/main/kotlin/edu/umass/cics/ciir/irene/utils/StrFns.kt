package edu.umass.cics.ciir.irene.utils

/**
 * @author jfoley
 */
fun String.splitAt(c: Char): Pair<String, String>? {
    val pos = this.indexOf(c)
    if (pos < 0) return null
    return Pair(this.substring(0, pos), this.substring(pos+1))
}

fun String.maybeSplitAt(c: Char): Pair<String, String?> {
    val pos = this.indexOf(c)
    if (pos < 0) return Pair(this, null)
    return Pair(this.substring(0, pos), this.substring(pos+1))
}
