package it.polimi.affetti.tspoon.tgraph.twopc;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by affo on 17/01/18.
 *
 * Does nothing
 */
public class NoWAL implements WAL {
    @Override
    public void open() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void addEntry(Entry entry) {

    }

    private Iterator<Entry> emptyItr() {
        return new Iterator<Entry>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Entry next() {
                return null;
            }
        };
    }

    @Override
    public Iterator<Entry> replay(String namespace) {
        return emptyItr();
    }

    @Override
    public Iterator<Entry> replay(int sourceID, int numberOfSources) throws IOException {
        return emptyItr();
    }

    @Override
    public void startSnapshot(long newWM) throws IOException {

    }

    @Override
    public void commitSnapshot() throws IOException {

    }

    @Override
    public long getSnapshotInProgressWatermark() throws IOException {
        return 0L;
    }
}
