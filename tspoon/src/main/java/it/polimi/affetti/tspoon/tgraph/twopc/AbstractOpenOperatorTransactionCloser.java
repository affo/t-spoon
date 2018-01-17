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
    protected boolean isDurabilityEnabled = false;
    protected transient WAL wal;

    protected AbstractOpenOperatorTransactionCloser(SubscriptionMode subscriptionMode) {
        super(subscriptionMode);
    }

    public void enableDurability() {
        isDurabilityEnabled = true;
    }

    @Override
    public synchronized void open() throws Exception {
        super.open();
        if (isDurabilityEnabled) {
            // TODO send to kafka
            // up to now, we only introduce overhead by writing to disk
            wal = new DummyWAL("wal.log");
        } else {
            wal = new NoWAL();
        }

        wal.open();
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

    protected void writeToWAL(int timestamp, Vote vote, String updates) {
        try {
            switch (vote) {
                case REPLAY:
                    wal.replay(timestamp);
                    break;
                case ABORT:
                    wal.abort(timestamp);
                    break;
                default:
                    wal.commit(timestamp, updates);
            }
        } catch (IOException e) {
            // make it crash, we cannot avoid persisting the WAL
            throw new RuntimeException("Cannot persist to WAL");
        }
    }
}
