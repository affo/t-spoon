package it.polimi.affetti.tspoon.tgraph.twopc;

/**
 * Created by affo on 02/12/17.
 */
public class AtOpenListener extends AbstractListener<OpenOperatorTransactionCloseListener>
        implements OpenOperatorTransactionCloseListener {
    public final static String prefix = "> AtOpen:\t";

    public AtOpenListener(
            AbstractTwoPCParticipant<OpenOperatorTransactionCloseListener> closer,
            AbstractTwoPCParticipant.SubscriptionMode subscriptionMode) {
        super(closer, subscriptionMode);
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public OpenOperatorTransactionCloseListener getListener() {
        return this;
    }

    @Override
    public void onCloseTransaction(CloseTransactionNotification notification) {
        queue.addMessage(notification);
    }

    @Override
    public boolean isInterestedIn(long timestamp) {
        return subscriber.isInterestedIn(timestamp);
    }
}
