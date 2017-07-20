package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.tgraph.TransactionContext;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

/**
 * Created by affo on 20/07/17.
 * <p>
 * Represents a piece of a distributed transaction on a given state operator and
 * on a particular key.
 * <p>
 * Defines what to do on transaction execution and termination.
 * It is instantiated by {@link TransactionContext}'s and contains the
 * necessary context for the transaction to execute.
 * <p>
 * Both `execute` and `terminate` are guaranteed
 * to execute atomically wrt the object considered during this transaction.
 */
public abstract class TransactionExecution<T, V> {
    protected String key;
    protected StateFunction<T, V> stateFunction;
    protected SafeCollector<T, Update<V>> collector;

    public TransactionExecution(String key, StateFunction<T, V> stateFunction, SafeCollector<T, Update<V>> collector) {
        this.key = key;
        this.stateFunction = stateFunction;
        this.collector = collector;
    }

    protected ObjectHandler<V> affectState(T element, ObjectVersion<V> version) {
        ObjectHandler<V> handler;

        if (version.object != null) {
            handler = new ObjectHandler<>(stateFunction.copyValue(version.object));
        } else {
            handler = new ObjectHandler<>(stateFunction.defaultValue());
        }

        stateFunction.apply(element, handler);

        return handler;
    }

    protected abstract void execute(Object<V> versions, T element);

    protected abstract void terminate(Vote status);

    public String getKey() {
        return key;
    }
}
