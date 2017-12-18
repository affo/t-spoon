package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Created by affo on 20/12/17.
 */
public class PessimisticTransactionExecutor implements
        KeyLevelTaskExecutor.TaskCompletionObserver<PessimisticTransactionExecutor.OperationExecutionResult> {
    private KeyLevelTaskExecutor<OperationExecutionResult> executor;
    // taskId -> callback
    private Map<Long, Consumer<OperationExecutionResult>> callbacks;

    public PessimisticTransactionExecutor(int numberOfExecutors) {
        this(numberOfExecutors, -1);
    }

    public PessimisticTransactionExecutor(int numberOfExecutors, long deadlockTimeout) {
        callbacks = new ConcurrentHashMap<>();

        executor = new KeyLevelTaskExecutor<>(numberOfExecutors, this);
        if (deadlockTimeout > 0) {
            executor.enableDeadlockDetection(deadlockTimeout);
        }
        executor.startProcessing();
    }

    /**
     * NOTE: __Consideration on global tracking of versions__
     * Pessimistic method does not need a global tracking for versions.
     * It is enough to store a unique incremental id per operator instance (-> kv storage shard):
     *
     * <pre>
     * {@code
     * private int localVersionId = 1;
     * ...
     * executor.run( ...., () -> {
     *      ObjectVersion<V> nextVersion = handler.createVersion(metadata.tid, localVersionId); // or something similar
     * }
     * ...
     * localVersionId++;
     * }
     * </pre>
     *
     * The above implementation requires a modification in the StateOperator when calculating
     * the updates for the transaction. It has to know that the version of the object is
     * different from the timestamp.
     * The above implementation, however, removes the dependency towards a global tracking.
     *
     * At the moment, we use a global timestamp tracking in order to provide a uniform querying strategy.
     * Queries, indeed, run in snapshot isolation mode by relying on a global order of versions given by the timestamp.
     * Note that, at PL3, the order in which transaction run is not necessarily the timestamp's one,
     * because it depends on locking and non-deterministic scheduling (in optimistic mode, a transaction with lower
     * timestamp is replayed with a higher timestamp if out-of-order...). This makes external queries read non-properly
     * isolated results, resulting in an overall PL2 isolation level.
     *
     * At PL4, transaction ordering matches the timestamp, so there is no problem with external querying.
     */
    public <V> void executeOperation(String key, Transaction<V> transaction,
                                     Consumer<OperationExecutionResult> callback) {
        Supplier<OperationExecutionResult> task = () -> {
            Object<V> object = transaction.getObject(key);
            ObjectVersion<V> version = object.getLastCommittedVersion();
            ObjectHandler<V> handler = version.createHandler();

            transaction.getOperation(key).accept(handler);
            Vote vote = handler.applyInvariant() ? Vote.COMMIT : Vote.ABORT;
            transaction.mergeVote(vote);
            object.addVersion(transaction.tid, transaction.timestamp, handler.object);

            OperationExecutionResult taskResult = new OperationExecutionResult(vote);
            if (!handler.write) {
                taskResult.setReadOnly();
                transaction.setReadOnly(true);
            }

            return taskResult;
        };

        long id = executor.add(key, task);
        callbacks.put(id, callback);
        executor.run(id);
    }

    public <V> void onGlobalTermination(Transaction<V> transaction) {
        for (String key : transaction.getKeys()) {
            executor.free(key);
        }
    }

    @Override
    public void onTaskCompletion(long id, OperationExecutionResult taskResult) {
        this.callbacks.remove(id).accept(taskResult);
    }

    @Override
    public void onTaskDeadlock(long id) {
        this.callbacks.remove(id).accept(new OperationExecutionResult(Vote.REPLAY));
    }

    public class OperationExecutionResult extends KeyLevelTaskExecutor.TaskResult {
        public final Vote vote;

        private OperationExecutionResult(Vote vote) {
            this.vote = vote;
        }
    }
}
