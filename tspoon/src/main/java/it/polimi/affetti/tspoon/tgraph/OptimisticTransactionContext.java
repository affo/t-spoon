package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.tgraph.state.OptimisticTransactionExecution;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.TransactionExecution;
import it.polimi.affetti.tspoon.tgraph.state.Update;

/**
 * Created by affo on 16/03/17.
 */
public class OptimisticTransactionContext extends TransactionContext {
    public int ts;
    public int watermark;
    public int precedence = -1;

    @Override
    public <T, V> TransactionExecution<T, V> getTransactionExecution(
            String key, StateFunction<T, V> stateFunction, SafeCollector<T, Update<V>> collector) {
        return new OptimisticTransactionExecution<>(key, stateFunction, collector, this);
    }

    @Override
    public String toString() {
        return super.toString() + "\n+++ {" +
                "ts=" + ts +
                ", watermark=" + watermark +
                ", precedence=" + precedence +
                '}';
    }
}
