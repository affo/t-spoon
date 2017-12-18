package it.polimi.affetti.tspoon.tgraph.query;

/**
 * Created by affo on 02/08/17.
 */
public interface QueryVisitor {
    QueryResult visit(Query query);

    QueryResult visit(RandomQuery query);

    <T> QueryResult visit(PredicateQuery<T> query);
}
