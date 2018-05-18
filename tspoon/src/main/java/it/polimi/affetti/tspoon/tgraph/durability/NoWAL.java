package it.polimi.affetti.tspoon.tgraph.durability;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by affo on 17/01/18.
 *
 * Does nothing
 */
public class NoWAL implements WALService {
    @Override
    public void open() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    private Iterator<WALEntry> emptyItr() {
        return new Iterator<WALEntry>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public WALEntry next() {
                return null;
            }
        };
    }

    @Override
    public Iterator<WALEntry> replay(String namespace) {
        return emptyItr();
    }

    @Override
    public Iterator<WALEntry> replay(int sourceID, int numberOfSources) throws IOException {
        return emptyItr();
    }
}
