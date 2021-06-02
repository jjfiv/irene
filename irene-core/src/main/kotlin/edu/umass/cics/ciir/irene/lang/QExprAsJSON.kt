package edu.umass.cics.ciir.irene.lang

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.SimpleModule
import edu.umass.cics.ciir.irene.DataNeeded

class QExprModule : SimpleModule() {
    init {
        addSerializer(QExprSerializer())
        addDeserializer(QExpr::class.java, QExprDeserializer())
    }
}


/**
 *
 * @author jfoley.
 */
class QExprSerializer : JsonSerializer<QExpr>() {
    override fun handledType(): Class<QExpr> {
        return QExpr::class.java
    }
    override fun serialize(q: QExpr, gen: JsonGenerator, serializers: SerializerProvider) {
        fun writeChildren(q: QExpr) {
            gen.writeArrayFieldStart("children")
            for (child in q.children) {
                serialize(child, gen, serializers)
            }
            gen.writeEndArray()
        }
        gen.writeStartObject()
        when (q) {
            is MultiExpr -> {
                gen.writeStringField("kind", "Multi")
                gen.writeObjectFieldStart("named_exprs")
                for ((name, expr) in q.namedExprs) {
                    gen.writeObjectFieldStart(name)
                    serialize(expr, gen, serializers)
                    gen.writeEndObject()
                }
                gen.writeEndObject()
            }
            is ConstScoreExpr -> {
                gen.writeStringField("kind", "ConstScore")
                gen.writeNumberField("value", q.x)
            }
            is ConstCountExpr -> {
                gen.writeStringField("kind", "ConstCount")
                gen.writeNumberField("value", q.x)
            }
            is ConstBoolExpr -> {
                gen.writeStringField("kind", "ConstBool")
                gen.writeBooleanField("value", q.x)
            }
            AlwaysMatchLeaf -> gen.writeStringField("kind", "AlwaysMatchLeaf")
            NeverMatchLeaf -> gen.writeStringField("kind", "NeverMatchLeaf")
            is WhitelistMatchExpr -> {
                gen.writeStringField("kind", "WhitelistMatch")
                q.docIdentifiers?.let { it ->
                    gen.writeArrayFieldStart("ids")
                    for (num in it) {
                        gen.writeNumber(num)
                    }
                    gen.writeEndArray()
                }
                q.docNames?.let { it ->
                    gen.writeArrayFieldStart("names")
                    for (name in it) {
                        gen.writeString(name)
                    }
                    gen.writeEndArray()
                }
            }
            is DenseLongField -> {
                gen.writeStringField("kind", "DenseLongField")
                gen.writeStringField("name", q.name)
                gen.writeNumberField("missing", q.missing)
            }
            is DenseFloatField -> {
                gen.writeStringField("kind", "DenseFloatField")
                gen.writeStringField("name", q.name)
                gen.writeNumberField("missing", q.missing)
            }
            is LengthsExpr -> {
                gen.writeStringField("kind", "Lengths")
                gen.writeStringField("stats_field", q.statsField)
            }
            is TextExpr -> {
                gen.writeStringField("kind", "Text")
                gen.writeStringField("text", q.text)
                gen.writeStringField("field", q.field)
                gen.writeStringField("stats_field", q.statsField)
                gen.writeStringField("needed", q.needed.name)
            }
            is LuceneExpr -> {
                gen.writeStringField("kind", "Lucene")
                gen.writeStringField("query", q.rawQuery)
            }
            is SynonymExpr -> {
                gen.writeStringField("kind", "Synonym")
                writeChildren(q)
            }
            is AndExpr -> {
                gen.writeStringField("kind", "And")
                writeChildren(q)
            }
            is OrExpr -> {
                gen.writeStringField("kind", "Or")
                writeChildren(q)
            }
            is CombineExpr -> {
                gen.writeStringField("kind", "Combine")
                writeChildren(q)
                gen.writeArrayFieldStart("weights")
                q.weights.forEach { gen.writeNumber(it) }
                gen.writeEndArray()
            }
            is MultExpr -> {
                gen.writeStringField("kind", "Mult")
                writeChildren(q)
            }
            is MaxExpr -> {
                gen.writeStringField("kind", "Max")
                writeChildren(q)
            }
            is SmallerCountExpr -> {
                gen.writeStringField("kind", "SmallerCount")
                writeChildren(q)
            }
            is UnorderedWindowCeilingExpr -> {
                gen.writeStringField("kind", "UnorderedWindowCeiling")
                gen.writeNumberField("width", q.width)
                writeChildren(q)
            }
            is OrderedWindowExpr -> {
                gen.writeStringField("kind", "OrderedWindow")
                gen.writeNumberField("step", q.step)
                writeChildren(q)
            }
            is UnorderedWindowExpr -> {
                gen.writeStringField("kind", "UnorderedWindow")
                gen.writeNumberField("width", q.width)
                writeChildren(q)
            }
            is ProxExpr -> {
                gen.writeStringField("kind", "Prox")
                gen.writeNumberField("width", q.width)
                writeChildren(q)
            }
            is LongLTE -> TODO()
            is WeightExpr -> {
                gen.writeStringField("kind", "Weight")
                gen.writeObjectField("child", q.child)
                gen.writeNumberField("weight", q.weight)
            }
            is CountEqualsExpr -> {
                gen.writeStringField("kind", "CountEquals")
                gen.writeObjectField("child", q.child)
                gen.writeNumberField("target", q.target)
            }
            is LogValueExpr -> TODO()
            is DirQLExpr -> {
                gen.writeStringField("kind", "DirQL")
                gen.writeObjectField("child", q.child)
                q.mu?.let { gen.writeNumberField("mu", it) }
                q.stats?.let { gen.writeObjectField("stats", it) }
            }
            is LinearQLExpr -> {
                gen.writeStringField("kind", "LinearQL")
                gen.writeObjectField("child", q.child)
                q.lambda?.let { gen.writeNumberField("lambda", it) }
                q.stats?.let { gen.writeObjectField("stats", it) }
            }
            is AbsoluteDiscountingQLExpr -> TODO()
            is BM25Expr -> {
                gen.writeStringField("kind", "BM25")
                gen.writeObjectField("child", q.child)
                q.b?.let { gen.writeNumberField("b", it) }
                q.k?.let { gen.writeNumberField("k", it) }
                q.stats?.let { gen.writeObjectField("stats", it) }
                gen.writeBooleanField("extractedIDF", q.extractedIDF)
            }
            is CountToScoreExpr -> TODO()
            is BoolToScoreExpr -> TODO()
            is CountToBoolExpr -> TODO()
            is RM3Expr -> {
                gen.writeStringField("kind", "RM3")
                gen.writeNumberField("orig_weight", q.origWeight)
                gen.writeNumberField("fb_docs", q.fbDocs)
                gen.writeNumberField("fb_terms", q.fbTerms)
                gen.writeBooleanField("stopwords", q.stopwords)
                q.field?.let { gen.writeStringField("field", it) }
                q.field?.let { }
                gen.writeObjectField("child", q.child)
            }
        }
        gen.writeEndObject()
    }
}

private fun JsonNode.getStr(key: String): String {
    val obj = this.get(key)
    if (obj.isNull) {
        error("unexpected null for $key in $obj")
    } else if (!obj.isTextual) {
        error("expected text for $key in $obj")
    }
    return obj.textValue()
}
private fun JsonNode.stringOrNull(key: String): String? {
    val obj = this.get(key) ?: return null
    if (obj.isNull) {
        return null
    } else if (!obj.isTextual) {
        error("expected text for $key in $obj")
    }
    return obj.textValue()
}

private fun JsonNode.booleanOr(key: String, missing: Boolean): Boolean {
    val obj = this.get(key) ?: return missing
    if (obj.isNull) {
        return missing
    } else if (!obj.isBoolean) {
        error("expected boolean for key=$key in $this")
    }
    return obj.asBoolean(missing)
}
private fun JsonNode.doubleOrNull(key: String): Double? {
    val obj = this.get(key) ?: return null
    if (obj.isNull) {
        return null
    } else if (!obj.isNumber) {
        error("expected number for key=$key in $this")
    }
    return obj.doubleValue()
}
private fun JsonNode.getInt(key: String): Int {
    val obj = this.get(key)
    if (obj.isNull) {
        error("unexpected null for $key in $this")
    } else if (!obj.isNumber) {
        error("expected number for $key in $this")
    }
    return obj.intValue()
}
private fun JsonNode.getLong(key: String): Long {
    val obj = this.get(key)
    if (obj.isNull) {
        error("unexpected null for $key in $this")
    } else if (!obj.isNumber) {
        error("expected number for $key in $this")
    }
    return obj.longValue()
}
private fun JsonNode.statsOrNull(key: String="stats"): CountStats? {
    val obj = this.get(key) ?: return null
    if (obj.isNull) {
        return null
    } else if (!obj.isObject) {
        error("expected stats ($key) to be an object in $this")
    }
    return CountStats(
            obj.getStr("source"),
            obj.getLong("cf"),
            obj.getLong("df"),
            obj.getLong("cl"),
            obj.getLong("dc")
    )
}

class QExprDeserializer : JsonDeserializer<QExpr>() {
    fun interpret_children(obj: JsonNode, key: String="children"): List<QExpr> {
        val children = obj.get(key)
        if (children.isNull) {
            error("$key are null in $obj")
        } else if (!children.isArray) {
            error("$key should be an array!")
        } else {
            val output = arrayListOf<QExpr>()
            for (child in children.elements()) {
                output.add(interpret(child))
            }
            return output
        }
    }
    fun interpret_child(obj: JsonNode, key: String = "child"): QExpr {
        val child = obj.get(key)
        if (child.isNull) {
            error("$key is null in $obj")
        } else if (!child.isObject) {
            error("$key should be an object!")
        } else {
            return interpret(child)
        }
    }
    fun interpret(obj: JsonNode): QExpr {
        val kind = obj.getStr("kind")
        return when (kind) {
            "Lengths" -> LengthsExpr(obj.getStr("field"))
            "Must" -> MustExpr(
                must = interpret_child(obj, "must"),
                value = interpret_child(obj, "value")
            )
            "BM25" -> BM25Expr(
                    child = interpret_child(obj),
                    b = obj.doubleOrNull("b"),
                    k = obj.doubleOrNull("k"),
                    stats = obj.statsOrNull(),
                    extractedIDF = obj.booleanOr("extractedIDF", false),
            )
            "LinearQL" -> LinearQLExpr(
                    child = interpret_child(obj),
                    lambda = obj.doubleOrNull("lambda") ?: obj.doubleOrNull("lambda_"),
                    stats = obj.statsOrNull()
            )
            "DirQL" -> DirQLExpr(
                    child = interpret_child(obj),
                    mu = obj.doubleOrNull("mu"),
                    stats = obj.statsOrNull()
            )
            "DenseLongField" -> DenseLongField(
                name=obj.stringOrNull("name") ?: error("Required parameter: 'name'"),
                missing=obj.getInt("missing").toLong(),
            )
            "DenseFloatField" -> DenseFloatField(
                name=obj.stringOrNull("name") ?: error("Required parameter: 'name'"),
                missing=obj.doubleOrNull("missing")?.toFloat() ?: -Float.MAX_VALUE,
            )
            "CountEquals" -> CountEqualsExpr(
                child=interpret_child(obj),
                target=obj.getInt("target"),
            )
            "Text" -> TextExpr(
                    text = obj.getStr("text"),
                    field = obj.stringOrNull("field"),
                    statsField = obj.stringOrNull("stats_field"),
                    needed = when(obj.stringOrNull("needed")) {
                        null, "DOCS" -> DataNeeded.DOCS
                        "COUNTS" -> DataNeeded.COUNTS
                        "POSITIONS" -> DataNeeded.POSITIONS
                        "SCORES" -> DataNeeded.SCORES
                        else -> error("DataNeeded = ${obj.stringOrNull("needed")}")
                    }
            )
            "Lucene" -> {
                LuceneExpr(obj.getStr("query"))
            }
            "Combine" -> {
                val w_obj = obj.get("weights") ?: error("weights must be specified for combine $obj.")
                if (!w_obj.isArray) error("weights must be an array in combine: $obj")
                val weights = arrayListOf<Double>()
                for (item in w_obj.elements()) {
                    if (!item.isNumber) {
                        error("All weights must be a number: $item")
                    }
                    weights.add(item.asDouble())
                }
                CombineExpr(
                        children = interpret_children(obj),
                        weights=weights
                )
            }
            "OrderedWindow" -> OrderedWindowExpr(
                children=interpret_children(obj),
                step=obj.getInt("step")
            )
            "UnorderedWindow" -> UnorderedWindowExpr(
                children=interpret_children(obj),
                width=obj.getInt("width")
            )
            "Synonym" -> SynonymExpr(interpret_children(obj))
            "Or" -> OrExpr(interpret_children(obj))
            "And" -> AndExpr(interpret_children(obj))
            "Max" -> MaxExpr(interpret_children(obj))
            "Mult" -> MultExpr(interpret_children(obj))
            "Weight" -> WeightExpr(
                    child=interpret_child(obj), weight=obj.doubleOrNull("weight") ?: error("WeightExpr has null weight.")
            )
            "RM3" -> RM3Expr(
                    child=interpret_child(obj),
                    fbDocs=obj.getInt("fb_docs"),
                    fbTerms=obj.getInt("fb_terms"),
                    field=obj.stringOrNull("field"),
                    stopwords=obj.get("stopwords").asBoolean()
            )
            else -> TODO("QExprDeserializer kind=$kind obj=$obj")
        }
    }
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): QExpr {
        val obj: JsonNode = p.readValueAsTree()
        return interpret(obj)
    }
}
