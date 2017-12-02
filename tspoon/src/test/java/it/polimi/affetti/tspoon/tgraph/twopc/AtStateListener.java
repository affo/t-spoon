package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

/**
 * Created by affo on 02/12/17.
 */
public class AtStateListener extends WithMessageQueue<CloseTransactionNotification>
        implements StateCloseTransactionListener {
    public final static String prefix = ">> AtState:\t";
    private Address coordinatorAddress;

    public AtStateListener(Address coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
    }

    public void setVerbose() {
        super.setVerbose(prefix);
    }

    @Override
    public void onTransactionClosedSuccess(CloseTransactionNotification notification) {
        addMessage(notification);
    }

    @Override
    public void onTransactionClosedError(CloseTransactionNotification notification, Throwable error) {
        System.out.println(prefix + notification + " - " + error.getMessage());
    }

    @Override
    public String getUpdatesRepresentation(int timestamp) {
        return "[updates_for_transaction_" + timestamp;
    }

    @Override
    public Address getCoordinatorAddressForTransaction(int timestamp) {
        return coordinatorAddress;
    }
}
