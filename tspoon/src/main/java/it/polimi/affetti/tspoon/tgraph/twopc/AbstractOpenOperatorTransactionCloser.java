package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.WithServer;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by affo on 09/11/17.
 */
public abstract class AbstractOpenOperatorTransactionCloser implements CoordinatorTransactionCloser {
    protected List<CoordinatorCloseTransactionListener> listeners = new LinkedList<>();
    private transient WithServer server;

    @Override
    public void open() throws Exception {
        server = new WithServer(getServer());
        server.open();
    }

    @Override
    public void close() throws Exception {
        this.server.close();
    }

    @Override
    public void subscribe(CoordinatorCloseTransactionListener listener) {
        listeners.add(listener);
    }

    protected void notifyListeners(CloseTransactionNotification notification) {
        for (CoordinatorCloseTransactionListener listener : listeners) {
            listener.onCloseTransaction(notification);
        }
    }

    public Address getOpenServerAddress() {
        return server.getMyAddress();
    }

    protected abstract AbstractServer getServer();
}
