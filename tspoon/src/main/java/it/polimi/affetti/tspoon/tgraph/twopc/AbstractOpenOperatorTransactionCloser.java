package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.runtime.AbstractServer;
import it.polimi.affetti.tspoon.runtime.NetUtils;

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
    public NetUtils.SingletonServerType getServerType() {
        return NetUtils.SingletonServerType.OPEN;
    }

    @Override
    public Supplier<AbstractServer> getServerSupplier() {
        return this::getServer;
    }

    protected abstract AbstractServer getServer();
}
