package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.WithSingletonServer;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created by affo on 04/12/17.
 * <p>
 * This class is instantiated as a singleton and has to handle multi-threaded access.
 */
public abstract class AbstractTwoPCParticipant<L extends TwoPCParticipant.Listener> implements TwoPCParticipant<L> {
    protected final SubscriptionMode subscriptionMode;
    protected Map<Long, List<L>> specificListeners = new HashMap<>();
    protected Set<L> genericListeners = new HashSet<>();
    // singleton server enforces the existence of only one server
    // and handles multiple open and closes from different threads
    private WithSingletonServer server;

    protected AbstractTwoPCParticipant(SubscriptionMode subscriptionMode) {
        this.subscriptionMode = subscriptionMode;
    }

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
        return server == null ? null : server.getMyAddress();
    }

    @Override
    public synchronized void subscribeTo(long timestamp, L listener) {
        if (subscriptionMode != SubscriptionMode.SPECIFIC) {
            throw new IllegalStateException("Cannot subscribe specifically when subscription mode is "
                    + subscriptionMode);
        }

        specificListeners.computeIfAbsent(timestamp, ts -> new LinkedList<>()).add(listener);
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

    protected synchronized List<L> removeListeners(long timestamp) {
        return specificListeners.remove(timestamp);
    }

    protected void notifySpecificListeners(CloseTransactionNotification notification,
                                           Consumer<L> notificationLogic) throws NullPointerException {
        List<L> listeners = removeListeners(notification.timestamp);
        // no need to synchronize notification
        listeners.forEach(notificationLogic);
    }

    protected void notifyGenericListeners(CloseTransactionNotification notification,
                                          Consumer<L> notificationLogic) {
        for (L listener : genericListeners) {
            // synchronization from the outside to guarantee atomic operation
            synchronized (listener.getMonitorForUpdateLogic()) {
                if (listener.isInterestedIn(notification.timestamp)) {
                    notificationLogic.accept(listener);
                }
            }
        }
    }

    protected void notifyListeners(CloseTransactionNotification notification,
                                   Consumer<L> notificationLogic) {

        if (subscriptionMode == SubscriptionMode.SPECIFIC) {
            notifySpecificListeners(notification, notificationLogic);
        } else {
            notifyGenericListeners(notification, notificationLogic);
        }
    }

    /**
     * Returns the correct subset of listeners for the current notification
     *
     * @param notification
     * @return
     */
    protected Iterable<L> getListeners(CloseTransactionNotification notification) {
        if (subscriptionMode == SubscriptionMode.SPECIFIC) {
            return removeListeners(notification.timestamp);
        }

        return genericListeners.stream()
                .filter(l -> l.isInterestedIn(notification.timestamp))
                .collect(Collectors.toSet());
    }

    public enum SubscriptionMode {
        SPECIFIC, GENERIC
    }
}
