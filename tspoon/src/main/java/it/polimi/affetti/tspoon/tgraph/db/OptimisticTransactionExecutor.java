package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.state.*;

/**
 * Created by affo on 19/12/17.
 */
public class OptimisticTransactionExecutor {
    private VersioningStrategy versioningStrategy;
    private DependencyTrackingStrategy dependencyTrackingStrategy;

    public OptimisticTransactionExecutor(
            IsolationLevel isolationLevel, boolean useDependencyTracking) {
        DependencyTrackingStrategy baseDependencyTrackingStrategy = useDependencyTracking ?
                new StandardDependencyTrackingStrategy() :
                new NoDependencyTrackingStrategy();

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
            case PL3:
                versioningStrategy = new PL3Strategy();
                break;
            case PL4:
                versioningStrategy = new PL4Strategy();
                break;
        }

        switch (isolationLevel) {
            case PL4:
                dependencyTrackingStrategy = new PL4DependencyTrackingStrategy(baseDependencyTrackingStrategy);
                break;
            default:
                dependencyTrackingStrategy = baseDependencyTrackingStrategy;
                break;
        }
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
        ObjectVersion<V> version = versioningStrategy.readVersion(
                tid, timestamp, watermark, object);
        ObjectHandler<V> handler = version.createHandler();

        // execute operation
        transaction.getOperation(key).accept(handler);
        Vote vote = handler.applyInvariant() ? Vote.COMMIT : Vote.ABORT;

        if (handler.write) {
            if (!versioningStrategy.canWrite(tid, timestamp, watermark, object)) {
                vote = Vote.REPLAY;
            }
        }

        transaction.mergeVote(vote);

        // add dependencies
        dependencyTrackingStrategy.updateDependencies(transaction, object, version);

        // avoid wasting memory in case we generated an invalid version
        if (vote != Vote.REPLAY) {
            ObjectVersion<V> objectVersion = versioningStrategy.installVersion(tid, timestamp, object, handler.object);
            transaction.addVersion(key, objectVersion);
        }
    }
}
