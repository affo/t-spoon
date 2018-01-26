package it.polimi.affetti.tspoon.tgraph.query;

/**
 * Created by affo on 02/08/17.
 */
public class RandomQuery extends Query {
    public final int size;

    public RandomQuery(String nameSpace, QueryID queryID, int size) {
        super(nameSpace, queryID);
        this.size = size;
    }

    @Override
    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        return "RandomQuery{" +
                "size=" + size +
                '}' + super.toString();
    }
}
