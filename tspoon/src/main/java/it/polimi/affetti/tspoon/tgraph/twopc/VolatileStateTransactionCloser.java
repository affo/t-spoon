package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;

import java.util.function.Consumer;

/**
 * Created by affo on 10/11/17.
 */
public class VolatileStateTransactionCloser implements StateOperatorTransactionCloser {
    @Override
    public void open() throws Exception {
        // does nothing
    }

    @Override
    public void close() throws Exception {
        // does nothing
    }

    /**
     * @param coordinatorAddress
     * @param timestamp
     * @param request            the request to be sent (in case) to coordinator (complete of updates)
     */
    @Override
    public void closeTransaction(
            Address coordinatorAddress, int timestamp, String request,
            Consumer<Void> success, Consumer<Throwable> error) {
        success.accept(null);
    }
}
