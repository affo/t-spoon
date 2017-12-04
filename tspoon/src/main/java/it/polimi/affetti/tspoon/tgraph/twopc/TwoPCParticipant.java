package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

import java.io.Serializable;

/**
 * Created by affo on 04/12/17.
 */
public interface TwoPCParticipant<L extends TwoPCParticipant.Listener> extends Serializable {
    void subscribeTo(long timestamp, L listener);

    Address getServerAddress();

    // For lifecycle
    void open() throws Exception;

    void close() throws Exception;

    interface Listener extends Serializable {
    }
}
