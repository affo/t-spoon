package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Created by affo on 04/12/17.
 * <p>
 * The TRuntimeContext is passed to the relevant operators passing through the TransactionEnvironment.
 * It is serialized and deserialized by every operator once on the task manager.
 * <p>
 * It is used to obtain singleton instances of AbstractOpenOperatorTransactionCloser and
 * AbstractStateOperatorTransactionCloser.
 * <p>
 * There is no need to return singleton instances of CloseSinkTransactionCloser, because we want the
 * maximum degree of parallelism for closing transactions.
 */
public class TRuntimeContext implements Serializable {
    private static AbstractOpenOperatorTransactionCloser[] openOperatorTransactionCloserPool;
    private static AbstractStateOperatorTransactionCloser[] stateOperatorTransactionCloserPool;

    private AbstractTwoPCParticipant.SubscriptionMode subscriptionMode = AbstractTwoPCParticipant.SubscriptionMode.GENERIC;
    public boolean durable, useDependencyTracking;
    public IsolationLevel isolationLevel;
    public Strategy strategy;
    public int openServerPoolSize = 1, stateServerPoolSize = 1, queryServerPoolSize = 1;

    public void setDurabilityEnabled(boolean durable) {
        this.durable = durable;
    }

    public boolean isDurabilityEnabled() {
        return durable;
    }

    public void setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public boolean isDependencyTrackingEnabled() {
        return useDependencyTracking;
    }

    public void setUseDependencyTracking(boolean useDependencyTracking) {
        this.useDependencyTracking = useDependencyTracking;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setSubscriptionMode(AbstractTwoPCParticipant.SubscriptionMode subscriptionMode) {
        this.subscriptionMode = subscriptionMode;
    }

    public AbstractTwoPCParticipant.SubscriptionMode getSubscriptionMode() {
        return subscriptionMode;
    }

    public <T> TransactionsIndex<T> getTransactionsIndex() {
        if (getStrategy() == Strategy.OPTIMISTIC && getIsolationLevel() == IsolationLevel.PL4) {
            return new TidForWatermarkingTransactionsIndex<>();
        }

        return new StandardTransactionsIndex<>();
    }

    public void setOpenServerPoolSize(int openServerPoolSize) {
        Preconditions.checkArgument(openServerPoolSize > 0,
                "Server pool size must be greater than 0");
        this.openServerPoolSize = openServerPoolSize;
    }

    public void setStateServerPoolSize(int stateServerPoolSize) {
        Preconditions.checkArgument(stateServerPoolSize > 0,
                "Server pool size must be greater than 0");
        this.stateServerPoolSize = stateServerPoolSize;
    }

    public void setQueryServerPoolSize(int queryServerPoolSize) {
        Preconditions.checkArgument(queryServerPoolSize > 0,
                "Server pool size must be greater than 0");
        this.queryServerPoolSize = queryServerPoolSize;
    }

    // ---------------------- These methods are called upon deserialization

    public AbstractOpenOperatorTransactionCloser getSourceTransactionCloser(int taskNumber) {
        Preconditions.checkArgument(taskNumber >= 0);

        int index = taskNumber % openServerPoolSize;

        synchronized (TRuntimeContext.class) {
            if (openOperatorTransactionCloserPool == null) {
                openOperatorTransactionCloserPool = new AbstractOpenOperatorTransactionCloser[openServerPoolSize];
            }

            if (openOperatorTransactionCloserPool[index] == null) {
                if (isDurabilityEnabled()) {
                    openOperatorTransactionCloserPool[index] = new DurableOpenOperatorTransactionCloser(subscriptionMode);
                } else {
                    openOperatorTransactionCloserPool[index] = new VolatileOpenOperatorTransactionCloser(subscriptionMode);
                }
            }

            return openOperatorTransactionCloserPool[index];
        }
    }


    public AbstractStateOperatorTransactionCloser getAtStateTransactionCloser(int taskNumber) {
        Preconditions.checkArgument(taskNumber >= 0);

        int index = taskNumber % stateServerPoolSize;

        synchronized (TRuntimeContext.class) {
            if (stateOperatorTransactionCloserPool == null) {
                stateOperatorTransactionCloserPool = new AbstractStateOperatorTransactionCloser[stateServerPoolSize];
            }

            if (stateOperatorTransactionCloserPool[index] == null) {
                if (isDurabilityEnabled()) {
                    stateOperatorTransactionCloserPool[index] = new DurableStateTransactionCloser(subscriptionMode);
                } else {
                    stateOperatorTransactionCloserPool[index] = new VolatileStateTransactionCloser(subscriptionMode);
                }
            }

            return stateOperatorTransactionCloserPool[index];
        }
    }

    // no singleton
    public CloseSinkTransactionCloser getSinkTransactionCloser() {
        if (isDurabilityEnabled()) {
            return new DurableSinkTransactionCloser();
        }

        return new VolatileSinkTransactionCloser();
    }
}
