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
public class SynchronousStateTransactionCloser extends AbstractStateOperatorTransactionCloser {
    private transient StringClientsCache clientsCache;
    private transient ExecutorService pool;

    protected SynchronousStateTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    public void open() throws Exception {
        super.open();
        clientsCache = new StringClientsCache();
        // TODO think about parallelizing more the ACK phase
        // ... with only 1 thread we ensure that we preserve order
        pool = Executors.newFixedThreadPool(1);
    }

    @Override
    public void close() throws Exception {
        super.close();
        clientsCache.clear();
        pool.shutdown();
    }

    // called once per TM
    @Override
    protected void onClose(Address coordinatorAddress, String request,
                           Consumer<Void> success, Consumer<Throwable> error) {
        StringClient coordinator;
        try {
            coordinator = clientsCache.getOrCreateClient(coordinatorAddress);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot connect to coordinator: " + coordinatorAddress);
        }

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
    }
}
