package it.polimi.affetti.tspoon.tgraph.twopc;

/**
 * Created by affo on 04/12/17.
 */
public interface OpenOperatorTransactionCloseListener extends TwoPCParticipant.Listener {
    void onCloseTransaction(CloseTransactionNotification notification);
}
