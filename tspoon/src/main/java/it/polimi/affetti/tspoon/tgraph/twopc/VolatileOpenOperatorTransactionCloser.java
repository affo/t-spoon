package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.ProcessRequestServer;

/**
 * Created by affo on 09/11/17.
 */
public class VolatileOpenOperatorTransactionCloser extends AbstractOpenOperatorTransactionCloser {
    @Override
    protected AbstractServer getServer() {
        return new OpenServer();
    }

    private class OpenServer extends ProcessRequestServer {
        @Override
        protected void parseRequest(String request) {
            // LOG.info(request);
            CloseTransactionNotification notification = CloseTransactionNotification.deserialize(request);
            notifyListeners(notification, (listener) -> listener.onCloseTransaction(notification));
        }
    }
}
