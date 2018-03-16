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
    protected final boolean isDurabilityEnabled;
    private transient WAL wal;

    protected AbstractCloseOperatorTransactionCloser(boolean isDurabilityEnabled) {
        this.isDurabilityEnabled = isDurabilityEnabled;
    }

    public void open() throws Exception {
        clients = new StringClientsCache();

        if (isDurabilityEnabled) {
            // TODO send to kafka
            // up to now, we only introduce overhead by writing to disk
            wal = new DummyWAL("wal.log");
        } else {
            wal = new NoWAL();
        }

        wal.open();
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
    public void writeToWAL(int timestamp, Vote vote, Updates updates) {
        if (!isDurabilityEnabled) {
            return;
        }

        try {
            switch (vote) {
                case REPLAY:
                    wal.replay(timestamp);
                    break;
                case ABORT:
                    wal.abort(timestamp);
                    break;
                default:
                    wal.commit(timestamp, updates);
            }
        } catch (IOException e) {
            // make it crash, we cannot avoid persisting the WAL
            throw new RuntimeException("Cannot persist to WAL");
        }
    }

    public void onMetadata(Metadata metadata) throws Exception {
        applyProtocolOnMetadata(metadata);
        writeToWAL(metadata.timestamp, metadata.vote, metadata.updates);
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
