package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.NetUtils;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Created by affo on 04/12/17.
 */
public interface TwoPCParticipant<L extends TwoPCParticipant.Listener> {
    /**
     * This is a specific subscription. The observer subscribes to a specific
     * `timestamp` and it is called once a notification for that one is received.
     * <p>
     * The observer does not need to filter out useless notifications, because it is
     * interested in every notification published to him
     * <p>
     * This method is called once every time that an observer is interested in a transaction
     * (a lot of times).
     *
     * @param timestamp
     * @param listener
     */
    void subscribeTo(long timestamp, L listener);

    /**
     * This is a generic subscription. The observer will be notified upon every possible notification
     * and will need to filter out the ones he is not interested in.
     * <p>
     * This method is called only once.
     * <p>
     * If the listener is an operator, the subscribe method should be called in the `open` method.
     *
     * @param listener
     */
    void subscribe(L listener);

    Address getServerAddress();

    NetUtils.ServerType getServerType();

    Supplier<AbstractServer> getServerSupplier();

    // For lifecycle
    void open() throws Exception;

    void close() throws Exception;

    interface Listener extends Serializable {
        int getTGraphID();
    }
}
