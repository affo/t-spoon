package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.common.SafeCollector;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.OptimisticTransactionContext;
import it.polimi.affetti.tspoon.tgraph.Transactions;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;

/**
 * Created by affo on 20/07/17.
 */
public class OptimisticTransactionExecution<T, V> extends TransactionExecution<T, V> {
    private OptimisticTransactionContext transactionContext;
    private ReadWriteStrategy strategy;
    private Object<V> versions;

    public OptimisticTransactionExecution(
            String key, StateFunction<T, V> stateFunction,
            SafeCollector<T, Update<V>> collector, OptimisticTransactionContext transactionContext) {
        super(key, stateFunction, collector);
        this.transactionContext = transactionContext;
        switch (Transactions.isolationLevel) {
            case PL0:
                strategy = new PL0Strategy();
                break;
            case PL1:
                strategy = new PL1Strategy();
                break;
            case PL2:
                strategy = new PL2Strategy();
                break;
            default:
                strategy = new PL3Strategy();
                break;
        }
    }

    @Override
    protected void execute(Object<V> versions, T element) {
        this.versions = versions;

        ObjectVersion<V> obj = strategy.extractVersion(transactionContext, versions);
        ObjectHandler<V> handler = affectState(element, obj);
        // add in any case, if replay it will be deleted later
        ObjectVersion<V> nextVersion = handler.object(transactionContext.getTid());
        versions.addVersion(key, nextVersion);
        int lastVersion = versions.getLastVersionBefore(Integer.MAX_VALUE).version;

        if (handler.read) {
            if (!strategy.isReadOK(transactionContext, lastVersion)) {
                transactionContext.twoPC.vote = Vote.REPLAY;
            }
        }

        if (handler.write) {
            if (!strategy.isWriteOK(transactionContext, lastVersion)) {
                transactionContext.twoPC.vote = Vote.REPLAY;
            }
        }

        transactionContext.twoPC.vote = stateFunction.invariant(nextVersion.object) ? Vote.COMMIT : Vote.ABORT;

        collector.safeCollect(Enriched.of(transactionContext, element));
    }

    @Override
    public void terminate(Vote status) {
        if (status == Vote.COMMIT) {
            ObjectVersion<V> version = versions.getLastVersionBefore(transactionContext.getTid());
            Update<V> update = Update.of(transactionContext.getTid(), getKey(), version.object);
            collector.safeCollect(update);
        } else {
            versions.deleteVersion(getKey(), transactionContext.getTid());
        }
    }
}
