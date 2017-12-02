package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;
import it.polimi.affetti.tspoon.runtime.WithServer;

/**
 * Created by affo on 09/11/17.
 */
public class VolatileCoordinatorTransactionCloser implements CoordinatorTransactionCloser {
    private CoordinatorCloseTransactionListener listener;
    private transient WithServer server;

    @Override
    public void open(CoordinatorCloseTransactionListener listener) throws Exception {
        this.listener = listener;

        server = new WithServer(new OpenServer());
        server.open();
    }

    @Override
    public void close() throws Exception {
        this.server.close();
    }

    public Address getAddress() {
        return server.getMyAddress();
    }

    private class OpenServer extends ProcessRequestServer {
        @Override
        protected void parseRequest(String request) {
            // LOG.info(request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);
            listener.onCloseTransaction(notification);
        }
    }
}
