package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Created by affo on 09/11/17.
 */
public abstract class AbstractOpenOperatorTransactionCloser extends
        AbstractTwoPCParticipant<OpenOperatorTransactionCloseListener> {

    protected AbstractOpenOperatorTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    @Override
    public synchronized void open() throws Exception {
        super.open();
    }

    @Override
    public NetUtils.ServerType getServerType() {
        return NetUtils.ServerType.OPEN;
    }

    @Override
    public Supplier<AbstractServer> getServerSupplier() {
        return this::getServer;
    }

    protected abstract AbstractServer getServer();
}
