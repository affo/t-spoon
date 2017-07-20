package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.TransactionExecution;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.TwoPCData;

import java.io.Serializable;

/**
 * Created by affo on 13/07/17.
 */
public abstract class TransactionContext implements Serializable {
    public TwoPCData twoPC = new TwoPCData();

    public abstract <T, V> TransactionExecution<T, V> getTransactionExecution(
            String key, StateFunction<T, V> stateFunction, SafeCollector<T, Update<V>> collector);

    @Override
    public String toString() {
        return twoPC.toString();
    }

    public int getTid() {
        return twoPC.tid;
    }
}
