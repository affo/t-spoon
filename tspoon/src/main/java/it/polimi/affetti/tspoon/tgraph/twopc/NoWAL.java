package it.polimi.affetti.tspoon.tgraph.twopc;

import java.io.IOException;

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
    public void replay(int tid) throws IOException {

    }

    @Override
    public void abort(int tid) throws IOException {

    }

    @Override
    public void commit(int tid, String updates) throws IOException {

    }
}
