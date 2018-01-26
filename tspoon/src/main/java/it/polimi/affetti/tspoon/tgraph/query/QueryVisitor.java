package it.polimi.affetti.tspoon.tgraph.query;

/**
 * Created by affo on 02/08/17.
 *
 * NOTE: visits must populate the QueryResult of the passed Query.
 */
public interface QueryVisitor {
    void visit(Query query);

    void visit(RandomQuery query);

    <T> void visit(PredicateQuery<T> query);
}
