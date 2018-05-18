package it.polimi.affetti.tspoon.tgraph.durability;

import java.io.IOException;

/**
 * Created by affo on 31/07/17.
 */
public interface SnapshotService {
    void open() throws IOException;

    void close() throws IOException;

    void startSnapshot(long newWM) throws IOException;

    long getSnapshotInProgressWatermark() throws IOException;
}
