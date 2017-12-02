package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

import java.io.Serializable;

/**
 * Created by affo on 09/11/17.
 */
public interface CoordinatorTransactionCloser extends Serializable {
    // For lifecycle
    void open(CoordinatorCloseTransactionListener listener) throws Exception;

    void close() throws Exception;

    Address getAddress();
}
