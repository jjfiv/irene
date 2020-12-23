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

private val quoteMap = mapOf<String, String>(
    // http://en.wikipedia.org/wiki/Quotation_mark
    "\u2018" to "'",
    "\u201a" to "'",
    "\u2019" to "'",
    "\u201c" to "\"",
    "\u201d" to "\"",
    "\u201e" to "\"",
    // convert <<,>> to "
    "\u00ab" to "\"",
    "\u00bb" to "\"",
    // convert <,> to "
    "\u2039" to "\"",
    "\u203a" to "\"",

    // corner marks to single-quotes
    "\u300c" to "'",
    "\u300d" to "'"
)

fun CharSequence.normalizeQuotes(): CharSequence {
    val sb = StringBuilder()
    for (c in this) {
        val ch = c.toString()
        sb.append(quoteMap.getOrDefault(ch, ch))
    }
    return sb
}