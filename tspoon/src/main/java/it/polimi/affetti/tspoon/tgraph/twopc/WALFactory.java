package it.polimi.affetti.tspoon.tgraph.twopc;

import java.io.Serializable;

/**
 * Created by affo on 21/03/18.
 */
public class WALFactory implements Serializable {
    private final boolean isDurabilityEnabled;

    public WALFactory(boolean isDurabilityEnabled) {
        this.isDurabilityEnabled = isDurabilityEnabled;
    }

    public WAL getWAL() {
        if (isDurabilityEnabled) {
            // TODO connect to kafka topic
            // up to now, we only introduce overhead by writing to disk
            return new DummyWAL("wal.log");
        }

        return new NoWAL();
    }

    public boolean isDurabilityEnabled() {
        return isDurabilityEnabled;
    }
}
