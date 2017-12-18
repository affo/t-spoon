package it.polimi.affetti.tspoon.tgraph.query;

/**
 * Created by affo on 02/08/17.
 */
public interface QueryListener {
    QueryResult onQuery(Query query);

    Iterable<String> getNameSpaces();
}
