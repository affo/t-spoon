package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.WithServer;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.function.Consumer;

/**
 * Created by affo on 01/12/17.
 */
public abstract class AbstractStateOperationTransactionCloser
        extends AbstractTwoPCParticipant<StateOperatorTransactionCloseListener> {
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
    public Address getServerAddress() {
        return srv.getMyAddress();
    }

    protected abstract void onClose(Address coordinatorAddress, String request,
                                    Consumer<Void> success, Consumer<Throwable> error);

    private class TransactionCloseServer extends ProcessRequestServer {
        @Override
        protected void parseRequest(String request) {
            // LOG.info(srv.getMyAddress() + " " + request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);

            notifyListeners(notification,
                    (listener) -> {
                        String updatesRepresentation = notification.vote == Vote.COMMIT ?
                                listener.getUpdatesRepresentation(notification.timestamp) : "[]";
                        Address coordinatorAddress = listener.getCoordinatorAddressForTransaction(notification.timestamp);
                        onClose(coordinatorAddress, request + "," + updatesRepresentation,
                                (aVoid) -> listener.onTransactionClosedSuccess(notification),
                                (error) -> listener.onTransactionClosedError(notification, error)
                        );
                    });
        }
    }
}
