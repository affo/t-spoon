package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

import java.util.function.Consumer;

/**
 * Created by affo on 10/11/17.
 */
public class AsynchronousStateTransactionCloser extends AbstractStateOperatorTransactionCloser {
    protected AsynchronousStateTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    protected void onClose(Address coordinatorAddress, String request,
                           Consumer<Void> onSinkACK, Consumer<Void> onCoordinatorACK,
                           Consumer<Throwable> error) {
        onSinkACK.accept(null);
        onCoordinatorACK.accept(null);
    }
}
