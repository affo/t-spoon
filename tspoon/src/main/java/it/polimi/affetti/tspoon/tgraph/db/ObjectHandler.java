package it.polimi.affetti.tspoon.tgraph.db;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Created by affo on 14/07/17.
 */
public class ObjectHandler<T> implements Serializable {
    public T object;
    public boolean read, write;
    private final Predicate<T> invariant;

    public ObjectHandler(T object, Predicate<T> invariant) {
        this.object = object;
        this.invariant = invariant;
    }

    public T read() {
        read = true;
        return object;
    }

    public void write(T value) {
        write = true;
        object = value;
    }

    public boolean applyInvariant() {
        return invariant.test(object);
    }
}
