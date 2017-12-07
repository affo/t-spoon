package it.polimi.affetti.tspoon.tgraph.twopc;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by affo on 07/12/17.
 */
public class Subscriber<L extends TwoPCParticipant.Listener> {
    private final Set<Long> interestedIn = new HashSet<>();
    private final AbstractTwoPCParticipant<L> closer;
    private final L listener;
    private final AbstractTwoPCParticipant.SubscriptionMode subscriptionMode;

    public Subscriber(AbstractTwoPCParticipant<L> closer,
                      L listener,
                      AbstractTwoPCParticipant.SubscriptionMode subscriptionMode) {
        this.closer = closer;
        this.listener = listener;
        this.subscriptionMode = subscriptionMode;
    }

    public boolean isInterestedIn(long timestamp) {
        return interestedIn.contains(timestamp);
    }

    public void subscribeTo(long timestamp) {
        if (subscriptionMode == AbstractTwoPCParticipant.SubscriptionMode.GENERIC) {
            if (interestedIn.isEmpty()) {
                closer.subscribe(listener);
            }
            interestedIn.add(timestamp);
        } else {
            closer.subscribeTo(timestamp, listener);
        }
    }
}
