package it.polimi.affetti.tspoon.runtime;

import it.polimi.affetti.tspoon.common.Address;

import java.util.function.Supplier;

/**
 * Created by affo on 20/07/17.
 */
public class WithSingletonServer {
    private final NetUtils.SingletonServerType serverType;
    private final Supplier<? extends AbstractServer> serverSupplier;
    private Address myAddress;

    public WithSingletonServer(
            NetUtils.SingletonServerType serverClass, Supplier<? extends AbstractServer> serverSupplier) {
        this.serverType = serverClass;
        this.serverSupplier = serverSupplier;
    }

    public void open() throws Exception {
        AbstractServer server = NetUtils.openAsSingleton(serverType, serverSupplier);
        if (myAddress == null) {
            myAddress = Address.of(server.getIP(), server.getPort());
        }
    }

    public void close() throws Exception {
        NetUtils.closeAsSingleton(serverType);
    }

    public Address getMyAddress() {
        return myAddress;
    }
}
