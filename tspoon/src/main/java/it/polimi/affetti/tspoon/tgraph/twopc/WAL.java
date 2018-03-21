package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.IOException;

/**
 * Created by affo on 31/07/17.
 */
public interface WAL {
    void open() throws IOException;

    void close() throws IOException;

    /**
     *
     * @param vote
     * @param timestamp
     * @param updates ignored if vote != COMMIT
     */
    void addEntry(Vote vote, int timestamp, Updates updates);

    void replay(int timestamp) throws IOException;

    void abort(int timestamp) throws IOException;

    void commit(int timestamp, Updates updates) throws IOException;
}
