package it.polimi.affetti.tspoon.tgraph.durability;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by affo on 31/07/17.
 */
public interface WALService {
    void open() throws IOException;

    void close() throws IOException;

    /**
     * Namespace can be '*' for every entry
     * @param namespace
     * @return
     * @throws IOException
     */
    Iterator<WALEntry> replay(String namespace) throws IOException;

    /**
     * For open operators
     *
     * @param sourceID
     * @param numberOfSources
     * @return
     * @throws IOException
     */
    Iterator<WALEntry> replay(int sourceID, int numberOfSources) throws IOException;
}
