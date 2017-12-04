package it.polimi.affetti.tspoon.tgraph.twopc;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by affo on 04/12/17.
 */
public abstract class AbstractTwoPCParticipant<L extends TwoPCParticipant.Listener> implements TwoPCParticipant<L> {
    protected Map<Long, List<L>> listeners = new HashMap<>();

    @Override
    public void subscribeTo(long timestamp, L listener) {
        listeners.computeIfAbsent(timestamp, ts -> new LinkedList<>()).add(listener);
    }

    protected void notifyListeners(CloseTransactionNotification notification,
                                   Consumer<L> notificationLogic) {
        List<L> listeners = this.listeners.remove((long) notification.timestamp);
        for (L listener : listeners) {
            notificationLogic.accept(listener);
        }
    }
}
