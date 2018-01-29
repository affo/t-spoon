package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

/**
 * Created by affo on 04/12/17.
 */
public interface StateOperatorTransactionCloseListener extends TwoPCParticipant.Listener {
    boolean isInterestedIn(long timestamp);

    void onTransactionClosedSuccess(CloseTransactionNotification notification);

    void onTransactionClosedError(
            CloseTransactionNotification notification, Throwable error);

    void pushTransactionUpdates(int timestamp);

    String getUpdatesRepresentation(int timestamp);

    Address getCoordinatorAddressForTransaction(int timestamp);
}
