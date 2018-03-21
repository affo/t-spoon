package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;

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
    public void addEntry(Vote vote, int timestamp, Updates updates) {

    }

    @Override
    public void replay(int timestamp) throws IOException {

    }

    @Override
    public void abort(int timestamp) throws IOException {

    }

    @Override
    public void commit(int timestamp, Updates updates) throws IOException {

    }
}
