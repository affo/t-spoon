package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.StringClient;
import it.polimi.affetti.tspoon.runtime.StringClientsCache;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Created by affo on 10/11/17.
 */
public class DurableStateTransactionCloser implements StateOperatorTransactionCloser {
    private transient StringClientsCache clientsCache;
    private transient ExecutorService pool;


    @Override
    public void open() throws Exception {
        clientsCache = new StringClientsCache();
        pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void close() throws Exception {
        clientsCache.clear();
        pool.shutdown();
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
        try {
            StringClient coordinator = clientsCache.getOrCreateClient(coordinatorAddress);

            coordinator.send(request);

            pool.submit(() -> {
                try {
                    // wait for the ACK
                    coordinator.receive();
                    success.accept(null);
                } catch (IOException e) {
                    error.accept(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Cannot create connection to coordinator " + coordinatorAddress);
        }
    }
}
