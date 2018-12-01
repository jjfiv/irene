package edu.umass.cics.ciir.irene.galago

import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.utils.normalize
import org.lemurproject.galago.core.index.mem.MemoryIndex
import org.lemurproject.galago.core.retrieval.FeatureFactory
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.core.retrieval.query.StructuredQuery
import org.lemurproject.galago.core.retrieval.traversal.*
import org.lemurproject.galago.core.retrieval.traversal.optimize.ExtentsToCountLeafTraversal
import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenCombineTraversal
import org.lemurproject.galago.core.retrieval.traversal.optimize.FlattenWindowTraversal
import org.lemurproject.galago.core.retrieval.traversal.optimize.MergeCombineChildrenTraversal
import org.lemurproject.galago.utility.Parameters
import javax.xml.soap.Text

internal val traversalParamList = listOf(
        // We can't support the RM traversal now.
        // RelevanceModelTraversal::class.java.name,
        ReplaceOperatorTraversal::class.java.name,
        StopStructureTraversal::class.java.name,
        StopWordTraversal::class.java.name,
        WeightedSequentialDependenceTraversal::class.java.name,
        SequentialDependenceTraversal::class.java.name,
        FullDependenceTraversal::class.java.name,
        ProximityDFRTraversal::class.java.name,
        // This traversal also does queries...
        // PRMS2Traversal::class.java.name,
        TransformRootTraversal::class.java.name,
        WindowRewriteTraversal::class.java.name,
        TextFieldRewriteTraversal::class.java.name,
        // Don't assign index parts!
        //PartAssignerTraversal::class.java.name,
        InsideToFieldPartTraversal::class.java.name,
        ImplicitFeatureCastTraversal::class.java.name,
        // Don't insert lengths!
        //InsertLengthsTraversal::class.java.name,
        PassageRestrictionTraversal::class.java.name,
        ExtentsToCountLeafTraversal::class.java.name,
        // Irene provides these:
        //FlattenWindowTraversal::class.java.name,
        //FlattenCombineTraversal::class.java.name,
        //MergeCombineChildrenTraversal::class.java.name,
        AnnotateParameters::class.java.name
        // Don't calculate statistics!
        //AnnotateCollectionStatistics::class.java.name,

        // Don't implement MaxScore
        //DeltaCheckTraversal::class.java.name
).map { traversalClassName ->
    pmake {
        set("order", "instead")
        set("name", traversalClassName)
    }
}
internal var lr = LocalRetrieval(MemoryIndex(), pmake { set("traversals", traversalParamList)})

internal fun parseGalagoQuery(input: String): GExpr = StructuredQuery.parse(input);
internal fun transformGalagoQuery(query: GExpr, queryParameters: Parameters): GExpr = lr.transformQuery(query, queryParameters)

fun GExpr.children(): List<GExpr> {
    val out = ArrayList<GExpr>()
    this.childIterator.forEach { out.add(it) }
    return out
}

/**
 * This method is mostly duplicating the work done in constructors, e.g.,
 * Galago's [org.lemurproject.galago.core.retrieval.iterator.ScoreCombinationIteratorScoreCombinationIterator] does weight resolution, so we have to do it here.
 */
internal fun galagoToIrene(expr: GExpr): QExpr {
    val np = expr.nodeParameters
    val children = expr.children();
    return when(expr.operator) {
        "combine" -> {
            val weights = ArrayList<Double>()
            for (i in 0..children.size) {
                weights.add(np.get("$i", 1.0))
            }

            val finalWeights = if (np.get("norm", true)) {
                weights.normalize()
            } else {
                weights
            }

            CombineExpr(children.map { galagoToIrene(it) }, finalWeights)
        }
        "dirichlet" -> {
            val mu = np.get("mu", 1500.0)
            DirQLExpr(transformOnlyOneChild(children), mu)
        }
        "inside" -> {
            assert(children.size == 2) {"Extents should have two children, found: $children"}
            val subExpr = galagoToIrene(children[0])
            assert(children[1].operator == "extents") {"The second child should tell us what field it is!: $children"}
            val field = children[1].nodeParameters.getString("default")
            // set this field recursively going down
            subExpr.map { node ->
                if (node is TextExpr) {
                    TextExpr(node.text, field, field, node.needed)
                } else {
                    node
                }
            }
        }
        "ordered", "od" -> OrderedWindowExpr(children.map { galagoToIrene(it) },
                np.get("default", 1L).toInt())
        "unordered", "uw" -> UnorderedWindowExpr(children.map { galagoToIrene(it) },
                np.get("default", 1L).toInt())
        "text", "extents", "counts" -> {
            TextExpr(np.getString("default"))
        }
        else -> throw UnsupportedOperationException("Cannot transform $expr.")
    }
}

internal fun transformOnlyOneChild(children: List<GExpr>): QExpr {
    if (children.size == 1) {
        return galagoToIrene(children[0])
    } else {
        error("Expected only one child, but found: ${children}")
    }
}

fun parseFromGalago(input: String, config: Parameters? = null): QExpr {
    val qp = config ?: Parameters.create()
    val parsed = parseGalagoQuery(input)
    val transformed = transformGalagoQuery(parsed, qp)
    return galagoToIrene(transformed)
}

fun main(args: Array<String>) {
    println("parseFromGalago ${parseFromGalago("#sdm(to.title be or not to be)")}")
}