package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;

import java.io.Serializable;

/**
 * Created by affo on 09/11/17.
 */
public interface CloseSinkTransactionCloser extends Serializable {
    // For lifecycle
    void open() throws Exception;

    void close() throws Exception;

    /**
     * Invoked every time a new transaction result has been gathered
     *
     * @param metadata
     */
    void onMetadata(Metadata metadata) throws Exception;
}
