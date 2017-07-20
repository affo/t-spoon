package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;

import java.io.Serializable;

/**
 * Created by affo on 17/07/17.
 */
public interface StateFunction<T, V> extends Serializable {
    V defaultValue();

    V copyValue(V value);

    boolean invariant(V value);

    void apply(T element, ObjectHandler<V> handler);
}
