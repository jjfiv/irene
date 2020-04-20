package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.galago.pmake
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

fun stats_from_json(p: Parameters, key: String="stats"): CountStats? {
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

fun maybe_stats_to_json(cs: CountStats?): Parameters? {
    val stats = cs ?: return null
    return pmake {
        put("source", stats.source)
        put("cf", stats.cf)
        put("df", stats.df)
        put("cl", stats.cl)
        put("dc", stats.dc)
    }
}

fun children_from_json(p: Parameters, key: String="children"): List<QExpr> {
    return p.getAsList(key).map { x ->
        when(x) {
            is Parameters -> expr_from_json(x)
            else -> error("Found $x as a child...")
        }
    }
}
fun child_from_json(p: Parameters, key: String="child"): QExpr {
    return expr_from_json(p.getMap(key))
}

fun expr_to_json(q: QExpr): Parameters {
    return when (q) {
        is MultiExpr -> TODO()
        is ConstScoreExpr -> TODO()
        is ConstCountExpr -> TODO()
        is ConstBoolExpr -> TODO()
        AlwaysMatchLeaf -> TODO()
        NeverMatchLeaf -> TODO()
        is WhitelistMatchExpr -> TODO()
        is DenseLongField -> TODO()
        is DenseFloatField -> TODO()
        is LengthsExpr -> pmake {
            put("kind", "Lengths")
            put("stats_field", q.statsField)
        }
        is TextExpr -> pmake {
            put("kind", "Text")
            putIfNotNull("text", q.text)
            putIfNotNull("field", q.field)
            putIfNotNull("stats_field", q.statsField)
        }
        is LuceneExpr -> TODO()
        is SynonymExpr -> pmake {
            put("kind", "Synonym")
            put("children", q.children.map { expr_to_json(it) })
        }
        is AndExpr -> pmake {
            put("kind", "And")
            put("children", q.children.map { expr_to_json(it) })
        }
        is OrExpr -> pmake {
            put("kind", "Or")
            put("children", q.children.map { expr_to_json(it) })
        }
        is CombineExpr -> pmake {
            put("kind", "Combine")
            put("children", q.children.map { expr_to_json(it) })
            put("weights", q.weights)
        }
        is MultExpr -> pmake {
            put("kind", "Mult")
            put("children", q.children.map { expr_to_json(it) })
        }
        is MaxExpr -> pmake {
            put("kind", "Max")
            put("children", q.children.map { expr_to_json(it) })
        }
        is SmallerCountExpr -> TODO()
        is UnorderedWindowCeilingExpr -> TODO()
        is OrderedWindowExpr -> pmake {
            put("kind", "OrderedWindow")
            put("step", q.step)
            put("children", q.children.map { expr_to_json(it) })
        }
        is UnorderedWindowExpr -> pmake {
            put("kind", "UnorderedWindow")
            put("width", q.width)
            put("children", q.children.map { expr_to_json(it) })
        }
        is ProxExpr -> TODO()
        is LongLTE -> TODO()
        is WeightExpr -> TODO()
        is CountEqualsExpr -> TODO()
        is LogValueExpr -> TODO()
        is DirQLExpr -> pmake {
            put("kind", "DirQL")
            put("child", expr_to_json(q.child))
            putIfNotNull("mu", q.mu)
            putIfNotNull("stats", maybe_stats_to_json(q.stats))
        }
        is LinearQLExpr -> TODO()
        is AbsoluteDiscountingQLExpr -> TODO()
        is BM25Expr -> TODO()
        is CountToScoreExpr -> TODO()
        is BoolToScoreExpr -> TODO()
        is CountToBoolExpr -> TODO()
        is RM3Expr -> pmake {
            put("kind", "RM3")
            put("orig_weight", q.origWeight)
            put("fb_docs", q.fbDocs)
            put("fb_terms", q.fbTerms)
            put("stopwords", q.stopwords)
            putIfNotNull("field", q.field)
            put("child", expr_to_json(q.child))
        }
        is RequireExpr -> TODO()
        is MustExpr -> TODO()
    }
}

fun expr_from_json(p: Parameters): QExpr {
    return when(p.get("kind")) {
        null -> error("invalid JSON, missing kind! $p")
        "Lengths" -> LengthsExpr(
            statsField=p.getString("field")
        )
        "BM25" -> BM25Expr(
                child=child_from_json(p),
                b=p.doubleOrNull("b"),
                k=p.doubleOrNull("k"),
                stats=stats_from_json(p))
        "LinearQL" -> LinearQLExpr(
                child=child_from_json(p),
                lambda=p.doubleOrNull("lambda") ?: p.doubleOrNull("lambda_"),
                stats=stats_from_json(p))
        "DirQL" -> DirQLExpr(
                child=child_from_json(p),
                mu=p.doubleOrNull("mu"),
                stats=stats_from_json(p))
        "Text" -> TextExpr(
                text=p.getStr("text"),
                field=p.getString("field"),
                statsField=p.getString("stats_field"))
        "Combine" -> CombineExpr(
                children=children_from_json(p),
                weights=p.doubleList("weights")
        )
        "OrderedWindow" -> OrderedWindowExpr(
                children=children_from_json(p),
                step=p.getInt("step")
        )
        "UnorderedWindow" -> UnorderedWindowExpr(
                children=children_from_json(p),
                width=p.getInt("width")
        )
        "Synonym" -> SynonymExpr(children_from_json(p))
        "Max" -> MaxExpr(children_from_json(p))
        "Mult" -> MultExpr(children_from_json(p))

        "Weight" -> WeightExpr(
                child=child_from_json(p),
                weight=p.getDouble("weight")
        )
        "RM3" -> RM3Expr(
                child=child_from_json(p),
                origWeight=p.getDouble("orig_weight"),
                fbDocs=p.getInt("fb_docs"),
                fbTerms=p.getInt("fb_terms"),
                field=p.getString("field"),
                stopwords=p.get("stopwords", true)
        )
        else -> error("TODO: $p")
    }
}
