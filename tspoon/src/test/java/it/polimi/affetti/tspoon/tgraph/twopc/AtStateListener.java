package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

/**
 * Created by affo on 02/12/17.
 */
public class AtStateListener extends AbstractListener<StateOperatorTransactionCloseListener>
        implements StateOperatorTransactionCloseListener {
    public final static String prefix = ">> AtState:\t";
    private Address coordinatorAddress;

    public AtStateListener(
            Address coordinatorAddress,
            AbstractTwoPCParticipant<StateOperatorTransactionCloseListener> closer,
            AbstractTwoPCParticipant.SubscriptionMode subscriptionMode) {
        super(closer, subscriptionMode);
        this.coordinatorAddress = coordinatorAddress;
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public StateOperatorTransactionCloseListener getListener() {
        return this;
    }

    @Override
    public void onTransactionClosedSuccess(CloseTransactionNotification notification) {
        queue.addMessage(notification);
    }

    @Override
    public void onTransactionClosedError(CloseTransactionNotification notification, Throwable error) {
        System.out.println(prefix + notification + " - " + error.getMessage());
    }

    @Override
    public void pushTransactionUpdates(int timestamp) {
        // does nothing
    }

    @Override
    public String getUpdatesRepresentation(int timestamp) {
        return "[updates_for_transaction_" + timestamp;
    }

    @Override
    public Address getCoordinatorAddressForTransaction(int timestamp) {
        return coordinatorAddress;
    }

    @Override
    public Object getMonitorForUpdateLogic() {
        return this;
    }

    @Override
    public boolean isInterestedIn(long timestamp) {
        return subscriber.isInterestedIn(timestamp);
    }
}
