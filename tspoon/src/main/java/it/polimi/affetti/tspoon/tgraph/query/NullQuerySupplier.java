package it.polimi.affetti.tspoon.tgraph.query;

/**
 * Created by affo on 29/05/17.
 */
public class NullQuerySupplier implements QuerySupplier {
    @Override
    public Query getQuery(QueryID queryID) {
        return null;
    }
}
