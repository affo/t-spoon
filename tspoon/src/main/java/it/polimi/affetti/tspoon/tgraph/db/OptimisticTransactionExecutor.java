package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.state.*;

/**
 * Created by affo on 19/12/17.
 */
public class OptimisticTransactionExecutor {
    private IsolationLevel isolationLevel;
    private VersioningStrategy versioningStrategy;
    private boolean useDependencyTracking;

    public OptimisticTransactionExecutor(
            IsolationLevel isolationLevel, boolean useDependencyTracking) {
        this.isolationLevel = isolationLevel;

        switch (isolationLevel) {
            case PL0:
                versioningStrategy = new PL0Strategy();
                break;
            case PL1:
                versioningStrategy = new PL1Strategy();
                break;
            case PL2:
                versioningStrategy = new PL2Strategy();
                break;
            case PL4:
                versioningStrategy = new PL4Strategy();
                break;
            default:
                versioningStrategy = new PL3Strategy();
                break;
        }

        this.useDependencyTracking = useDependencyTracking;
    }

    public <V> void executeOperation(String key, Transaction<V> transaction) {
        if (transaction.vote != Vote.COMMIT) {
            // do not process not COMMITted transactions
            return;
        }

        int tid = transaction.tid;
        int timestamp = transaction.timestamp;
        int watermark = transaction.watermark;

        Object<V> object = transaction.getObject(key);
        ObjectVersion<V> version = versioningStrategy.extractObjectVersion(
                tid, timestamp, watermark, object);
        ObjectHandler<V> handler = version.createHandler();

        // execute operation
        transaction.getOperation(key).accept(handler);
        Vote vote = handler.applyInvariant() ? Vote.COMMIT : Vote.ABORT;

        if (handler.write) {
            if (!versioningStrategy.isWritingAllowed(tid, timestamp, watermark, object)) {
                vote = Vote.REPLAY;
            }
        }

        // add dependencies
        if (isolationLevel == IsolationLevel.PL4 ||
                (useDependencyTracking && vote == Vote.REPLAY)) {
            transaction.addDependencies(
                    versioningStrategy.extractDependencies(tid, timestamp, watermark, object));
        }

        // avoid wasting memory in case we generated an invalid version
        if (vote != Vote.REPLAY) {
            object.addVersion(tid, timestamp, handler.object);
        }

        transaction.mergeVote(vote);
    }
}
