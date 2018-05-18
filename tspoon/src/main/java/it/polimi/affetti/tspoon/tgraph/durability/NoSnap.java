package it.polimi.affetti.tspoon.tgraph.durability;

import java.io.IOException;

/**
 * Created by affo on 17/05/18.
 */
public class NoSnap implements SnapshotService {
    @Override
    public void open() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void startSnapshot(long newWM) throws IOException {

    }

    @Override
    public long getSnapshotInProgressWatermark() throws IOException {
        return 0;
    }
}
