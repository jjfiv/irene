package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.irene.*
import org.apache.lucene.queryparser.classic.QueryParser

/**
 * @author jfoley.
 */
class IreneQueryLanguage(val index: IIndex = EmptyIndex()) : RREnv() {
    // TODO, true:
    override var optimizeBM25 = true
    override var optimizeDirLog = true
    override var defaultField: String
        get() = index.defaultField
        set(value) {
            error("Crap!")
        }

    override fun lookupNames(docNames: Set<String>): List<Int> = docNames.mapNotNull { index.documentById(it) }
    override fun fieldStats(field: String): CountStats = index.fieldStats(field) ?: error("Requested field $field does not exist.")
    override fun computeStats(q: QExpr): CountStats = index.getStats(q)
    override fun getStats(term: String, field: String?): CountStats = index.getStats(term, field ?: defaultField)
    override fun search(q: QExpr, limit: Int): List<IreneWeightedDoc> {
        if (q.requiresPRF()) {
            error("Cannot run a query that requires PRF expansion through this API!\n$q")
        }
        return index.search(q, limit).scoreDocs.map { IreneWeightedDoc(it.score, it.doc) }
    }

    override fun lookupTerms(doc: Int, field: String): List<String> =
        index.terms(doc, field)


    val luceneQueryParser: QueryParser
        get() = QueryParser(defaultField, (index as IreneIndex).analyzer)
    override var estimateStats: String? = null
}

