package it.polimi.affetti.tspoon.tgraph.twopc;

/**
 * Created by affo on 07/12/17.
 */
public abstract class AbstractListener<L extends TwoPCParticipant.Listener> {
    protected final Subscriber<L> subscriber;
    protected final WithMessageQueue<CloseTransactionNotification> queue = new WithMessageQueue<>();


    public AbstractListener(AbstractTwoPCParticipant<L> closer,
                            AbstractTwoPCParticipant.SubscriptionMode subscriptionMode) {
        subscriber = new Subscriber<>(closer, getListener(), subscriptionMode);
    }

    public abstract String getPrefix();

    public abstract L getListener();

    public void setVerbose() {
        queue.setVerbose(getPrefix());
    }

    public CloseTransactionNotification receive() throws InterruptedException {
        return queue.receive();
    }

    public void subscribeTo(long timestamp) {
        subscriber.subscribeTo(timestamp);
    }
}
