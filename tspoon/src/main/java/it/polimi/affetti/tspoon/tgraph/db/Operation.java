package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.tgraph.state.StateFunction;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Created by affo on 19/12/17.
 */
public interface Operation<V> extends Consumer<ObjectHandler<V>>, Serializable {
    static <T, V> Operation<V> from(T element, StateFunction<T, V> stateFunction) {
        return handler -> stateFunction.apply(element, handler);
    }
}
