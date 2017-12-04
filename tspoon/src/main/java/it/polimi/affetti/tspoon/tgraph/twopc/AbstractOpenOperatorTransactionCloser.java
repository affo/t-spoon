package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.WithServer;

/**
 * Created by affo on 09/11/17.
 */
public abstract class AbstractOpenOperatorTransactionCloser extends
        AbstractTwoPCParticipant<OpenOperatorTransactionCloseListener> {
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
    public Address getServerAddress() {
        return server.getMyAddress();
    }

    protected abstract AbstractServer getServer();
}
