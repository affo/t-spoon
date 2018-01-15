package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Strategy;

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
    private static AbstractOpenOperatorTransactionCloser openOperatorTransactionCloser;
    private static AbstractStateOperatorTransactionCloser stateOperatorTransactionCloser;

    private AbstractTwoPCParticipant.SubscriptionMode subscriptionMode = AbstractTwoPCParticipant.SubscriptionMode.GENERIC;
    public boolean durable, useDependencyTracking;
    public IsolationLevel isolationLevel;
    public Strategy strategy;

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

    // ---------------------- These methods are called upon deserialization

    public AbstractOpenOperatorTransactionCloser getSourceTransactionCloser() {
        synchronized (TRuntimeContext.class) {
            if (openOperatorTransactionCloser == null) {
                if (isDurabilityEnabled()) {
                    openOperatorTransactionCloser = new DurableOpenOperatorTransactionCloser(subscriptionMode);
                } else {
                    openOperatorTransactionCloser = new VolatileOpenOperatorTransactionCloser(subscriptionMode);
                }
            }

            return openOperatorTransactionCloser;
        }
    }

    public AbstractStateOperatorTransactionCloser getAtStateTransactionCloser() {
        synchronized (TRuntimeContext.class) {
            if (stateOperatorTransactionCloser == null) {
                if (isDurabilityEnabled()) {
                    stateOperatorTransactionCloser = new DurableStateTransactionCloser(subscriptionMode);
                } else {
                    stateOperatorTransactionCloser = new VolatileStateTransactionCloser(subscriptionMode);
                }
            }

            return stateOperatorTransactionCloser;
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
