package it.polimi.affetti.tspoon.tgraph.query;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Created by affo on 02/08/17.
 */
public class PredicateQuery<T> extends Query {
    public final QueryPredicate<T> predicate;

    public PredicateQuery() {
        this.predicate = new SelectNone<>();
    }

    public PredicateQuery(String nameSpace, QueryPredicate<T> predicate) {
        super(nameSpace);
        this.predicate = predicate;
    }

    public boolean test(T value) {
        return predicate.test(value);
    }

    @Override
    public void accept(QueryVisitor visitor) {
        this.result = visitor.visit(this);
    }

    public interface QueryPredicate<T> extends Predicate<T>, Serializable {
    }

    public static class SelectAll<T extends Object> implements QueryPredicate<T> {

        @Override
        public boolean test(T anObject) {
            return true;
        }
    }

    public static class SelectNone<T extends Object> implements QueryPredicate<T> {

        @Override
        public boolean test(T anObject) {
            return false;
        }
    }
}
