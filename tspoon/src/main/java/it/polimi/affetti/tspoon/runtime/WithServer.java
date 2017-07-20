package it.polimi.affetti.tspoon.runtime;

import it.polimi.affetti.tspoon.common.Address;

/**
 * Created by affo on 20/07/17.
 */
public class WithServer {
    private AbstractServer server;
    private Address myAddress;

    public WithServer(AbstractServer server) {
        this.server = server;
    }

    public void open() throws Exception {
        server = NetUtils.getServer(NetUtils.MIN_PORT, NetUtils.MAX_PORT, server);
        myAddress = Address.of(server.getIP(), server.getPort());
    }

    public void close() throws Exception {
        server.close();
    }

    public Address getMyAddress() {
        return myAddress;
    }
}
