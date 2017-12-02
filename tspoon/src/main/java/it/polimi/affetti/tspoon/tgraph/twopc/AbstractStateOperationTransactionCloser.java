package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by affo on 01/12/17.
 */
public abstract class AbstractStateOperationTransactionCloser implements StateOperatorTransactionCloser {
    private List<StateCloseTransactionListener> listeners = new LinkedList<>();
    private transient WithServer srv;

    @Override
    public void open() throws Exception {
        srv = new WithServer(new TransactionCloseServer());
        srv.open();
    }

    @Override
    public void close() throws Exception {
        srv.close();
    }

    @Override
    public Address getStateServerAddress() {
        return srv.getMyAddress();
    }

    @Override
    public void subscribe(StateCloseTransactionListener listener) {
        listeners.add(listener);
    }

    protected abstract void onClose(Address coordinatorAddress, String request,
                                    Consumer<Void> success, Consumer<Throwable> error);

    private class TransactionCloseServer extends ProcessRequestServer {
        @Override
        protected void parseRequest(String request) {
            // LOG.info(srv.getMyAddress() + " " + request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);

            for (StateCloseTransactionListener listener : listeners) {
                String updatesRepresentation = notification.vote == Vote.COMMIT ?
                        listener.getUpdatesRepresentation(notification.timestamp) : "[]";
                Address coordinatorAddress = listener.getCoordinatorAddressForTransaction(notification.timestamp);
                onClose(coordinatorAddress, request + "," + updatesRepresentation,
                        (aVoid) -> listener.onTransactionClosedSuccess(notification),
                        (error) -> listener.onTransactionClosedError(notification, error)
                );
            }
        }
    }
}
