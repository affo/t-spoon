package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

/**
 * Created by affo on 09/11/17.
 */
public interface StateCloseTransactionListener {
    void onTransactionClosedSuccess(CloseTransactionNotification notification);

    void onTransactionClosedError(
            CloseTransactionNotification notification, Throwable error);

    String getUpdatesRepresentation(int timestamp);
    Address getCoordinatorAddressForTransaction(int timestamp);
}
