package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;

/**
 * Created by affo on 10/11/17.
 */
public abstract class AbstractTwoPCFactory implements TwoPCFactory {

    @Override
    public TransactionsIndex getTransactionsIndex() {
        if (TransactionEnvironment.isolationLevel == IsolationLevel.PL4) {
            return new TidTransactionsIndex();
        }

        return new StandardTransactionsIndex();
    }

    @Override
    public CoordinatorTransactionCloser getSourceTransactionCloser() {
        if (TransactionEnvironment.isDurabilityEnabled) {
            return new DurableCoordinatorTransactionCloser();
        }

        return new VolatileCoordinatorTransactionCloser();
    }

    @Override
    public CloseSinkTransactionCloser getSinkTransactionCloser() {
        if (TransactionEnvironment.isDurabilityEnabled) {
            return new DurableSinkTransactionCloser();
        }

        return new VolatileSinkTransactionCloser();
    }

    @Override
    public StateOperatorTransactionCloser getAtStateTransactionCloser() {
        if (TransactionEnvironment.isDurabilityEnabled) {
            return new DurableStateTransactionCloser();
        }

        return new VolatileStateTransactionCloser();
    }
}
