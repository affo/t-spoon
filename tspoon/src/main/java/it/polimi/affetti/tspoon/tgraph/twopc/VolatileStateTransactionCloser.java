package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

import java.util.function.Consumer;

/**
 * Created by affo on 10/11/17.
 */
public class VolatileStateTransactionCloser extends AbstractStateOperatorTransactionCloser {
    protected VolatileStateTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    protected void onClose(Address coordinatorAddress, String request,
                           Consumer<Void> success, Consumer<Throwable> error) {
        success.accept(null);
    }
}
