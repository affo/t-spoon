package it.polimi.affetti.tspoon.tgraph.query;

/**
 * Created by affo on 02/08/17.
 */
public interface QueryVisitor {
    void visit(Query query);

    void visit(RandomQuery query);

    <T> void visit(PredicateQuery<T> query);
}
