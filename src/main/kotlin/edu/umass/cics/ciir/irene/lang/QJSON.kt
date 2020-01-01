package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.galago.getStr
import org.lemurproject.galago.utility.Parameters

fun Parameters.getK(key: String): Any? {
    val x = this.get(key) ?: return null
    if (x is Parameters.NullMarker) return null
    return x
}

fun Parameters.doubleOrNull(key: String): Double? {
    val x = this.getK(key) ?: return null
    return when(x) {
        is Number -> x.toDouble()
        else -> error("$key should be a double or null... is $x of class ${x.javaClass}")
    }
}
fun Parameters.doubleList(key: String): List<Double> {
    val x = this.getK(key)
    return when(x) {
        is List<*> -> x.map { when(it) {
            is Number -> it.toDouble()
            else -> error("Found $it in list of doubles!")
        } }
        else -> error("$key should be a list of doubles, was: $x!")
    }
}

fun stats_from_json(p: Parameters, key: String): CountStats? {
    val x = p.getK(key)
    return when(x) {
        null -> null
        is Parameters -> CountStats(
                p.getStr("source"),
                p.getLong("cf"),
                p.getLong("df"),
                p.getLong("cl"),
                p.getLong("dc")
        )
        else -> error("$key should be a double or null!")
    }
}

fun children_from_json(p: Parameters, key: String): List<QExpr> {
    return p.getAsList(key).map { x ->
        when(x) {
            is Parameters -> expr_from_json(x)
            else -> error("Found $x as a child...")
        }
    }
}

fun expr_from_json(p: Parameters): QExpr {
    return when(p.get("kind")) {
        null -> error("invalid JSON, missing kind! $p")
        "BM25" -> BM25Expr(
                expr_from_json(p.getMap("child")),
                p.doubleOrNull("b"),
                p.doubleOrNull("k"),
                stats_from_json(p, "stats"))
        "DirQL" -> DirQLExpr(
                expr_from_json(p.getMap("child")),
                p.doubleOrNull("mu"),
                stats_from_json(p, "stats"))
        "Text" -> TextExpr(
                text=p.getStr("text"),
                field=p.getString("field"),
                statsField=p.getString("stats_field"))
        "Combine" -> CombineExpr(
                children=children_from_json(p, "children"),
                weights=p.doubleList("weights")
        )
        "RM3" -> RM3Expr(
                child=expr_from_json(p.getMap("child")),
                origWeight=p.getDouble("orig_weight"),
                fbDocs=p.getInt("fb_docs"),
                fbTerms=p.getInt("fb_terms"),
                field=p.getString("field"),
                stopwords=p.get("stopwords", true)
        )
        else -> error("TODO: $p")
    }
}