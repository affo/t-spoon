package it.polimi.affetti.tspoon.tgraph.twopc;

/**
 * Created by affo on 02/12/17.
 */
public class AtOpenListener extends WithMessageQueue<CloseTransactionNotification>
        implements OpenOperatorTransactionCloseListener {
    public final static String prefix = "> AtOpen:\t";

    public void setVerbose() {
        super.setVerbose(prefix);
    }

    @Override
    public void onCloseTransaction(CloseTransactionNotification notification) {
        addMessage(notification);
    }
}
