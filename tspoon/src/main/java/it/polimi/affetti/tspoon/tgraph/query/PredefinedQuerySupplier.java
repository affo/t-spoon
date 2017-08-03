package it.polimi.affetti.tspoon.tgraph.query;

/**
 * Created by affo on 29/05/17.
 */
public class PredefinedQuerySupplier implements QuerySupplier {
    private Query predefQuery;

    public PredefinedQuerySupplier(Query predefQuery) {
        this.predefQuery = predefQuery;
    }

    @Override
    public Query getQuery() {
        return predefQuery;
    }
}
