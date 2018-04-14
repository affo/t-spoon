package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.StringClientsCache;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by affo on 09/11/17.
 */
public abstract class AbstractCloseOperatorTransactionCloser {
    protected transient StringClientsCache clients;
    private transient WAL wal;

    public void open(WAL wal) throws Exception {
        clients = new StringClientsCache();
        this.wal = wal;
        this.wal.open();
    }

    public void close() throws Exception {
        clients.clear();
        wal.close();
    }

    /**
     * No effect if durability is not enabled
     *
     * @param timestamp
     * @param vote
     * @param updates
     */
    public void writeToWAL(int tid, int timestamp, Vote vote, Updates updates) throws IOException {
        wal.addEntry(new WAL.Entry(vote, tid, timestamp, updates));
    }

    public void onMetadata(Metadata metadata) throws Exception {
        applyProtocolOnMetadata(metadata);
        writeToWAL(metadata.tid, metadata.timestamp, metadata.vote, metadata.updates);
    }

    /**
     * Invoked every time a new transaction result has been gathered
     *
     * @param metadata
     */
    abstract void applyProtocolOnMetadata(Metadata metadata) throws Exception;

    // ------------------------------ helper methods

    protected void send(Address address, String message) throws IOException {
        send(Collections.singleton(address), message);
    }

    protected void send(Iterable<Address> addresses, String message) throws IOException {
        for (Address address : addresses) {
            clients.getOrCreateClient(address).send(message);
        }
    }
}
