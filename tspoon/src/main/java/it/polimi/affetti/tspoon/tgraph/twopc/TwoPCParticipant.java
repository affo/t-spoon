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
    void subscribeTo(long timestamp, L listener);

    Address getServerAddress();

    NetUtils.SingletonServerType getServerType();

    Supplier<AbstractServer> getServerSupplier();

    // For lifecycle
    void open() throws Exception;

    void close() throws Exception;

    interface Listener extends Serializable {
    }
}
