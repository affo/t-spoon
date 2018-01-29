package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.TaskExecutor;
import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.state.*;
import org.apache.log4j.Logger;

import java.util.function.Consumer;

/**
 * Created by affo on 19/12/17.
 */
public class OptimisticTransactionExecutor {
    private Logger LOG = Logger.getLogger(OptimisticTransactionExecutor.class.getSimpleName());
    private VersioningStrategy versioningStrategy;
    private DependencyTrackingStrategy dependencyTrackingStrategy;
    private boolean needWaitOnRead;
    private TaskExecutor taskExecutor;

    public OptimisticTransactionExecutor(
            IsolationLevel isolationLevel,
            boolean useDependencyTracking,
            boolean needWaitOnRead) {
        this.needWaitOnRead = needWaitOnRead;

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

        if (needWaitOnRead) {
            this.taskExecutor = new TaskExecutor();
            this.taskExecutor.start();
        }
    }

    public <V> void executeOperation(String key, Transaction<V> transaction, Consumer<Void> andThen) {
        Consumer<Void> execution = aVoid -> {
            if (transaction.vote != Vote.COMMIT) {
                // do not process not COMMITted transactions
                return;
            }

            int tid = transaction.tid;
            int timestamp = transaction.timestamp;
            int watermark = transaction.watermark;

            Object<V> object = transaction.getObject(key);
            // the read could be deferred depending on the protocol and isolationLevel used
            ObjectVersion<V> version = versioningStrategy.readVersion(
                    tid, timestamp, watermark, object);
            ObjectHandler<V> handler = version.createHandler();

            // execute operation
            transaction.getOperation(key).accept(handler);
            Vote vote = handler.applyInvariant() ? Vote.COMMIT : Vote.ABORT;

            if (handler.write && !versioningStrategy.canWrite(tid, timestamp, watermark, object)) {
                vote = Vote.REPLAY;
            }

            transaction.mergeVote(vote);

            // add dependencies
            dependencyTrackingStrategy.updateDependencies(transaction, object, version);

            // avoid wasting memory in case we generated an invalid version
            if (vote != Vote.REPLAY) {
                ObjectVersion<V> objectVersion = versioningStrategy.installVersion(tid, timestamp, object, handler.object);
                transaction.addVersion(key, objectVersion);
            }
        };

        Consumer<Void> theTask = aVoid -> {
            execution.accept(null);
            andThen.accept(null);
        };

        if (!needWaitOnRead) {
            // simple execution
            theTask.accept(null);
        } else {
            // deferred execution
            taskExecutor.addTask(() -> theTask.accept(null));
        }
    }

    public void close() {
        if (taskExecutor != null) {
            taskExecutor.interrupt();
        }
    }
}
