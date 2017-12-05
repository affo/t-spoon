package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.WithSingletonServer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by affo on 04/12/17.
 * <p>
 * This class is instantiated as a singleton and has to handle multi-threaded access.
 */
public abstract class AbstractTwoPCParticipant<L extends TwoPCParticipant.Listener> implements TwoPCParticipant<L> {
    protected Map<Long, List<L>> listeners = new HashMap<>();
    // singleton server enforces the existence of only one server
    // and handles multiple open and closes from different threads
    private WithSingletonServer server;

    @Override
    public synchronized void open() throws Exception {
        if (server == null) {
            server = new WithSingletonServer(getServerType(), getServerSupplier());
            server.open();
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (server != null) {
            this.server.close();
            this.server = null; // make it possible to open and close the same TwoPCParticipant more than once
        }
    }

    @Override
    public synchronized Address getServerAddress() {
        return server.getMyAddress();
    }

    @Override
    public synchronized void subscribeTo(long timestamp, L listener) {
        listeners.computeIfAbsent(timestamp, ts -> new LinkedList<>()).add(listener);
    }

    protected synchronized List<L> removeListeners(long timestamp) {
        return listeners.remove(timestamp);
    }

    protected void notifyListeners(CloseTransactionNotification notification,
                                   Consumer<L> notificationLogic) {
        List<L> listeners = removeListeners(notification.timestamp);
        // no need to synchronize notification
        for (L listener : listeners) {
            notificationLogic.accept(listener);
        }
    }
}
