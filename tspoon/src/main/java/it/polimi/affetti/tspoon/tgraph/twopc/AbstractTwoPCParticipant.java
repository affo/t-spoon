package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.WithServer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Created by affo on 04/12/17.
 *
 * Instances of this type are provided in limited number by the TRuntimeContext and shared across threads.
 * So they must handle multi-threaded access.
 */
public abstract class AbstractTwoPCParticipant<L extends TwoPCParticipant.Listener> implements TwoPCParticipant<L> {
    protected final SubscriptionMode subscriptionMode;
    protected Map<Tuple2<Integer, Long>, List<L>> specificListeners = new HashMap<>();
    protected Set<L> genericListeners = new HashSet<>();
    private WithServer server;

    protected AbstractTwoPCParticipant(SubscriptionMode subscriptionMode) {
        this.subscriptionMode = subscriptionMode;
    }

    @Override
    public synchronized void open() throws Exception {
        if (server == null) {
            server = new WithServer(getServerSupplier().get(), getServerType());
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
        return server == null ? null : server.getMyAddress();
    }

    @Override
    public synchronized void subscribeTo(long timestamp, L listener) {
        if (subscriptionMode != SubscriptionMode.SPECIFIC) {
            throw new IllegalStateException("Cannot subscribe specifically when subscription mode is "
                    + subscriptionMode);
        }

        specificListeners.computeIfAbsent(Tuple2.of(listener.getTGraphID(), timestamp), k -> new LinkedList<>()).add(listener);
    }

    /**
     * I suppose that every call to this method is performed by the operators in the `open` method,
     * and that subscribers do not change during the job. If this happens, the iterations on genericListeners
     * could raise a ConcurrentModificationException.
     *
     * @param listener
     */
    @Override
    public synchronized void subscribe(L listener) {
        if (subscriptionMode != SubscriptionMode.GENERIC) {
            throw new IllegalStateException("Cannot subscribe generically when subscription mode is "
                    + subscriptionMode);
        }

        genericListeners.add(listener);
    }

    protected synchronized List<L> removeListeners(int tGraphID, long timestamp) {
        return specificListeners.remove(Tuple2.of(tGraphID, timestamp));
    }

    protected void notifyListeners(CloseTransactionNotification notification,
                                   Consumer<L> notificationLogic) {
        getListeners(notification).forEach(notificationLogic);
    }

    /**
     * Returns the correct subset of listeners for the current notification
     *
     * @param notification
     * @return
     */
    protected Stream<L> getListeners(CloseTransactionNotification notification) {
        Stream<L> listeners;
        if (subscriptionMode == SubscriptionMode.SPECIFIC) {
            listeners = removeListeners(notification.tGraphID, notification.timestamp).stream();
        } else {
            listeners = genericListeners.stream();
        }

        return listeners
                .filter(l -> l.getTGraphID() == notification.tGraphID);
    }

    public enum SubscriptionMode {
        SPECIFIC, GENERIC
    }
}
