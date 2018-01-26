package it.polimi.affetti.tspoon.tgraph.query;

import java.io.Serializable;

/**
 * Created by affo on 29/05/17.
 */
public interface QuerySupplier extends Serializable {
    Query getQuery(QueryID queryID);
}
