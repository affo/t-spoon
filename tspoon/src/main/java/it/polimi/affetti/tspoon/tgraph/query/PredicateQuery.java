package it.polimi.affetti.tspoon.tgraph.query;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Created by affo on 02/08/17.
 */
public class PredicateQuery<T> extends Query {
    public final QueryPredicate<T> predicate;

    public PredicateQuery(String nameSpace, QueryID queryID) {
        this(nameSpace, queryID, new SelectNone<>());
    }

    public PredicateQuery(String nameSpace, QueryID queryID, QueryPredicate<T> predicate) {
        super(nameSpace, queryID);
        this.predicate = predicate;
    }

    public boolean test(T value) {
        return predicate.test(value);
    }

    @Override
    public void accept(QueryVisitor visitor) {
        visitor.visit(this);
    }

    public interface QueryPredicate<T> extends Predicate<T>, Serializable {
    }

    public static class SelectAll<T> implements QueryPredicate<T> {

        @Override
        public boolean test(T anObject) {
            return true;
        }
    }

    public static class SelectNone<T> implements QueryPredicate<T> {

        @Override
        public boolean test(T anObject) {
            return false;
        }
    }
}
