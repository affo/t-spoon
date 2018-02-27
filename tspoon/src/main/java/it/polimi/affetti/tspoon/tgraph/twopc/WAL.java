package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Updates;

import java.io.IOException;

/**
 * Created by affo on 31/07/17.
 */
public interface WAL {
    void open() throws IOException;

    void close() throws IOException;

    void replay(int tid) throws IOException;

    void abort(int tid) throws IOException;

    void commit(int tid, Updates updates) throws IOException;
}
