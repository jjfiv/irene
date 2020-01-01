package edu.umass.cics.ciir.irene.galago

import edu.umass.cics.ciir.irene.utils.IntList
import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.IreneWeightedDoc
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.lang.RREnv
import org.lemurproject.galago.core.index.stats.FieldStatistics
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.utility.Parameters

/**
 * @author jfoley
 */
class RRGalagoEnv(val retr: LocalRetrieval) : RREnv() {
    override fun lookupNames(docNames: Set<String>): IntList {
        val output = IntList(docNames.size)
        retr.getDocumentIds(docNames.toList()).forEach {
            if (it > Int.MAX_VALUE) {
                error("Lucene only supports integer ids. To use Galago perfectly in parallel, please shard your index.")
            }
            output.push(it.toInt())
        }
        return output
    }

    init {
        defaultField = "document"
    }
    val lengthsInfo = HashMap<String, FieldStatistics>()
    private fun getFieldStats(field: String): FieldStatistics {
        return lengthsInfo.computeIfAbsent(field, {retr.getCollectionStatistics(GExpr("lengths", field))})
    }

    override fun fieldStats(field: String): CountStats {
        val fstats = getFieldStats(field)
        return CountStats("field=$field", cf=0, df=0, dc=fstats.documentCount, cl=fstats.collectionLength)
    }

    override fun computeStats(q: QExpr): CountStats {
        val field = q.getSingleStatsField(defaultField)
        val stats = retr.getNodeStatistics(retr.transformQuery(q.toGalago(this), Parameters.create()))
        val fstats = getFieldStats(field)
        return CountStats(q.toString(),
                cf = stats.nodeFrequency,
                df = stats.nodeDocumentCount,
                dc = fstats.documentCount,
                cl = fstats.collectionLength)
    }
    override fun getStats(term: String, field: String?): CountStats {
        val statsField = field ?: defaultField
        val fstats = getFieldStats(statsField)
        val termQ = GExpr("counts", term).apply { setf("field", statsField) }
        val stats = retr.getNodeStatistics(retr.transformQuery(termQ, Parameters.create()))!!
        return CountStats(termQ.toString(),
                cf = stats.nodeFrequency,
                df = stats.nodeDocumentCount,
                dc = fstats.documentCount,
                cl = fstats.collectionLength)
    }

    override fun search(q: QExpr, limit: Int): List<IreneWeightedDoc> {
        return retr.transformAndExecuteQuery(q.toGalago(this)).scoredDocuments.map { IreneWeightedDoc(it.score.toFloat(), it.document.toInt())}
    }

    override fun lookupTerms(doc: Int, field: String): List<String> {
        val name = retr.getDocumentName(doc.toLong()) ?: error("No such galago doc id=$doc")
        val gdoc = retr.getDocument(name, Document.DocumentComponents.JustTerms) ?: error("No such galago doc content name=$name id=$doc")
        return gdoc.terms ?: emptyList()
    }
}