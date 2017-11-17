package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.db.Object;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.db.ObjectVersion;
import it.polimi.affetti.tspoon.tgraph.twopc.StateOperatorTransactionCloser;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 10/11/17.
 * <p>
 * NOTE that implementing reads or write locks within a single state operator is useless because requires pre-processing
 * of the transaction itself. When a new transaction comes, if the key is either read or write locked, it waits for it to become
 * free. However we store if a transaction was read-only because this information can be used by the external queries
 * (that are read-only by definition).
 * <p>
 * This implies that, excluding external queries, internal transactions are inherently PL3 or PL4 isolated.
 */
public class PessimisticStateOperator<T, V> extends StateOperator<T, V>
        implements KeyLevelTaskExecutor.TaskCompletionObserver<PessimisticStateOperator.TaskResult> {
    private transient KeyLevelTaskExecutor<TaskResult> executor;
    private final Map<Long, Enriched<T>> deferredElements = new HashMap<>();

    // settings
    private boolean deadlockEnabled;
    private long deadlockTimeout;

    public PessimisticStateOperator(
            String nameSpace,
            StateFunction<T, V> stateFunction,
            OutputTag<Update<V>> updatesTag,
            StateOperatorTransactionCloser transactionCloser) {
        super(nameSpace, stateFunction, updatesTag, transactionCloser);
    }

    public void enableDeadlockDetection(long deadlockTimeout) {
        this.deadlockEnabled = true;
        this.deadlockTimeout = deadlockTimeout;
    }

    @Override
    public void open() throws Exception {
        super.open();
        executor = new KeyLevelTaskExecutor<>(1, this);
        if (deadlockEnabled) {
            executor.enableDeadlockDetection(deadlockTimeout);
        }
        executor.startProcessing();
    }

    @Override
    public void close() throws Exception {
        super.close();
        executor.stopProcessing();
    }

    /**
     * In the case of pessimistic operator different versions are useless, only last-stable and current-dirty.
     * In order to implement the trick, I always write the last version number + 1.
     */
    @Override
    protected void execute(TransactionContext tContext, String key, Object<V> object, Metadata metadata, T element) {
        // here's the trick
        tContext.version = (int) tContext.localId;

        executor.run(key,
                (taskId) -> deferredElements.put(taskId, Enriched.of(metadata, element)),
                () -> {
                    ObjectVersion<V> version = object.getLastCommittedVersion();
                    // TODO check why do I need this
                    ObjectHandler<V> handler;
                    if (version.object != null) {
                        handler = new ObjectHandler<>(stateFunction.copyValue(version.object));
                    } else {
                        handler = new ObjectHandler<>(stateFunction.defaultValue());
                    }
                    stateFunction.apply(element, handler);

                    ObjectVersion<V> nextVersion = handler.object(tContext.version);
                    object.addVersion(nextVersion);

                    Vote vote = stateFunction.invariant(nextVersion.object) ? Vote.COMMIT : Vote.ABORT;
                    TaskResult taskResult = new TaskResult(vote);
                    if (!handler.write) {
                        taskResult.setReadOnly();
                    }
                    return taskResult;
                });
    }

    @Override
    protected void onTermination(TransactionContext tContext) {
        for (String key : tContext.touchedObjects.keySet()) {
            executor.free(key);
        }
    }

    @Override
    public void onTaskCompletion(long id, TaskResult taskResult) {

        Enriched<T> element = deferredElements.remove(id);
        element.metadata.vote = element.metadata.vote.merge(taskResult.vote);
        collector.safeCollect(element);
    }

    @Override
    public void onTaskDeadlock(long id) {
        Enriched<T> element = deferredElements.remove(id);
        element.metadata.vote = Vote.REPLAY;
        collector.safeCollect(element);
    }

    public static class TaskResult extends KeyLevelTaskExecutor.TaskResult {
        public final Vote vote;

        private TaskResult(Vote vote) {
            this.vote = vote;
        }
    }
}
