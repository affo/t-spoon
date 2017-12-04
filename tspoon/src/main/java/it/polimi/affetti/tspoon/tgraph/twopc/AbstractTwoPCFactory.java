package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;

/**
 * Created by affo on 10/11/17.
 */
public abstract class AbstractTwoPCFactory implements TwoPCFactory {

    @Override
    public <T> TransactionsIndex<T> getTransactionsIndex() {
        if (TransactionEnvironment.get().getIsolationLevel() == IsolationLevel.PL4) {
            return new TidTransactionsIndex<>();
        }

        return new StandardTransactionsIndex<>();
    }

    @Override
    public AbstractOpenOperatorTransactionCloser getSourceTransactionCloser() {
        if (TransactionEnvironment.get().isDurabilityEnabled()) {
            return new DurableOpenOperatorTransactionCloser();
        }

        return new VolatileOpenOperatorTransactionCloser();
    }

    @Override
    public CloseSinkTransactionCloser getSinkTransactionCloser() {
        if (TransactionEnvironment.get().isDurabilityEnabled()) {
            return new DurableSinkTransactionCloser();
        }

        return new VolatileSinkTransactionCloser();
    }

    @Override
    public AbstractStateOperationTransactionCloser getAtStateTransactionCloser() {
        if (TransactionEnvironment.get().isDurabilityEnabled()) {
            return new DurableStateTransactionCloser();
        }

        return new VolatileStateTransactionCloser();
    }
}
