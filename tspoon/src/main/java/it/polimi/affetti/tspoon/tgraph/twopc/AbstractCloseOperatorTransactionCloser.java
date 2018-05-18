package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.StringClientsCache;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.durability.FileWAL;
import it.polimi.affetti.tspoon.tgraph.durability.WALEntry;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by affo on 09/11/17.
 */
public abstract class AbstractCloseOperatorTransactionCloser {
    protected transient StringClientsCache clients;
    private transient FileWAL wal;

    /**
     *
     * @param wal could be null in case durability is not enabled
     * @throws Exception
     */
    public void open(FileWAL wal) throws Exception {
        clients = new StringClientsCache();
        this.wal = wal;
    }

    public void close() throws Exception {
        clients.clear();
    }

    public void onMetadata(Metadata metadata) throws Exception {
        // write before applying protocol
        if (wal != null) {
            wal.addEntry(new WALEntry(metadata.vote, metadata.tid, metadata.timestamp, metadata.updates));
        }
        applyProtocolOnMetadata(metadata);
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
