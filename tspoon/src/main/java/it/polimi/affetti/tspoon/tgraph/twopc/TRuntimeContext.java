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
 * There is no need to return singleton instances of AbstractCloseOperatorTransactionCloser, because we want the
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
    private boolean synchronous;
    private boolean baselineMode;

    public void setDurabilityEnabled(boolean durable) {
        this.durable = durable;
    }

    public boolean isDurabilityEnabled() {
        return durable;
    }

    public WALFactory getWALFactory() {
        return new WALFactory(isDurabilityEnabled());
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

    public <T> TransactionsIndex<T> getTransactionsIndex(long startIndex, int sourceParallelism, int sourceID) {
        if (getStrategy() == Strategy.OPTIMISTIC && getIsolationLevel() == IsolationLevel.PL4) {
            return new TidForWatermarkingTransactionsIndex<>(startIndex, sourceParallelism, sourceID);
        }

        return new StandardTransactionsIndex<>(startIndex, sourceParallelism, sourceID);
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

    public void setSynchronous(boolean synchronous) {
        this.synchronous = synchronous;
    }

    public boolean isSynchronous() {
        return synchronous;
    }

    public void setBaselineMode(boolean baselineMode) {
        this.baselineMode = baselineMode;
    }

    public boolean isBaselineMode() {
        return baselineMode;
    }

    public boolean needWaitOnRead() {
        return !isSynchronous() && getIsolationLevel().gte(IsolationLevel.PL3);
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
                if (isSynchronous()) {
                    openOperatorTransactionCloserPool[index] = new SynchronousOpenOperatorTransactionCloser(subscriptionMode);
                } else {
                    openOperatorTransactionCloserPool[index] = new AsynchronousOpenOperatorTransactionCloser(subscriptionMode);
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
                if (isSynchronous()) {
                    stateOperatorTransactionCloserPool[index] = new SynchronousStateTransactionCloser(subscriptionMode);
                } else {
                    stateOperatorTransactionCloserPool[index] = new AsynchronousStateTransactionCloser(subscriptionMode);
                }
            }

            return stateOperatorTransactionCloserPool[index];
        }
    }

    /**
     * NOTE: Use it for testing only
     */
    void resetTransactionClosers() {
        stateOperatorTransactionCloserPool = null;
        openOperatorTransactionCloserPool = null;
    }

    // no singleton
    public AbstractCloseOperatorTransactionCloser getSinkTransactionCloser() {
        if (isSynchronous()) {
            return new SynchronousSinkTransactionCloser();
        }

        return new AsynchronousSinkTransactionCloser();
    }
}
