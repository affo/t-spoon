package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Provides results of executing transaction from a synchronous queue.
 * NOTE: Callback execution is synchronized.
 * I guarantee this
 */
public class PessimisticTransactionExecutor implements
        KeyLevelTaskExecutor.TaskCompletionObserver<PessimisticTransactionExecutor.OperationExecutionResult> {
    private KeyLevelTaskExecutor<OperationExecutionResult> executor;
    // taskId -> tid
    private Map<Long, Integer> taskTidMapping;
    // timestamp -> tasks (using timestamps for tracking to use a unique id)
    private Map<Integer, List<Long>> timestampTaskMapping;
    // taskId -> callback
    private Map<Long, Consumer<OperationExecutionResult>> callbacks;

    public PessimisticTransactionExecutor(int numberOfExecutors) {
        this(numberOfExecutors, -1);
    }

    public PessimisticTransactionExecutor(int numberOfExecutors, long deadlockTimeout) {
        taskTidMapping = new HashMap<>();
        timestampTaskMapping = new HashMap<>();
        callbacks = new HashMap<>();

        executor = new KeyLevelTaskExecutor<>(numberOfExecutors, this);
        if (deadlockTimeout > 0) {
            executor.enableDeadlockDetection(deadlockTimeout);
        }
        executor.startProcessing();
    }

    private synchronized void registerTask(long taskId, int tid, int timestamp, Consumer<OperationExecutionResult> callback) {
        taskTidMapping.put(taskId, tid);
        timestampTaskMapping.computeIfAbsent(timestamp, ts -> new LinkedList<>()).add(taskId);
        callbacks.put(taskId, callback);
    }

    private synchronized void unregisterTransaction(int timestamp) {
        List<Long> taskIds = timestampTaskMapping.remove(timestamp);
        taskIds.forEach(
                taskId -> {
                    taskTidMapping.remove(taskId);
                    callbacks.remove(taskId);
                }
        );
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
    public <V> void executeOperation(String key, Transaction<V> transaction, Consumer<OperationExecutionResult> callback) {
        Supplier<OperationExecutionResult> task = () -> {
            if (transaction.vote != Vote.COMMIT) {
                // no further processing for REPLAYed or ABORTed transactions
                OperationExecutionResult operationExecutionResult = new OperationExecutionResult(transaction.vote);
                for (Integer dependency : transaction.getDependencies()) {
                    operationExecutionResult.addDependency(dependency);
                }
                return operationExecutionResult;
            }

            Object<V> object = transaction.getObject(key);
            ObjectVersion<V> version = object.getLastCommittedVersion();
            ObjectHandler<V> handler = version.createHandler();

            transaction.getOperation(key).accept(handler);
            Vote vote = handler.applyInvariant() ? Vote.COMMIT : Vote.ABORT;
            transaction.mergeVote(vote);
            ObjectVersion<V> objectVersion = object.addVersion(transaction.tid, transaction.timestamp, handler.object);
            transaction.addVersion(key, objectVersion);

            OperationExecutionResult taskResult = new OperationExecutionResult(vote);
            if (!handler.write) {
                taskResult.setReadOnly();
                transaction.setReadOnly(true);
            }

            return taskResult;
        };

        long id = executor.add(key, task);
        registerTask(id, transaction.tid, transaction.timestamp, callback);
        executor.run(id);
    }

    public <V> void onGlobalTermination(Transaction<V> transaction) {
        List<Long> taskIds;
        synchronized (this) {
            taskIds = timestampTaskMapping.get(transaction.timestamp);
        }

        for (Long taskId : taskIds) {
            executor.ackCompletion(taskId);
        }

        unregisterTransaction(transaction.timestamp);
    }

    @Override
    public void onTaskCompletion(long id, OperationExecutionResult taskResult) {
        Consumer<OperationExecutionResult> callback;
        synchronized (this) {
            callback = callbacks.get(id);
        }

        callback.accept(taskResult);
    }

    @Override
    public void onDeadlock(LinkedHashMap<Long, List<Long>> deadlockedWithDependencies) {
        for (Map.Entry<Long, List<Long>> entry : deadlockedWithDependencies.entrySet()) {
            long taskId = entry.getKey();
            List<Long> dependencies = entry.getValue();

            OperationExecutionResult operationResult;
            Consumer<OperationExecutionResult> callback;
            synchronized (this) {
                operationResult = new OperationExecutionResult(Vote.REPLAY);
                for (Long dependency : dependencies) {
                    Integer tid = taskTidMapping.get(dependency);
                    if (tid == null) {
                        // It could happen that we detect a dependency with a task that run to completion
                        // and its transaction terminated globally.
                        // No matter if we detected a dependency with this transaction,
                        // we can skip tracking the dependency
                        continue;
                    }
                    operationResult.addDependency(tid);
                }

                callback = callbacks.get(taskId);
            }

            callback.accept(operationResult);
        }
    }

    public class OperationExecutionResult extends KeyLevelTaskExecutor.TaskResult {
        public final Vote vote;
        private final HashSet<Integer> replayCauses = new HashSet<>();

        public OperationExecutionResult(Vote vote) {
            this.vote = vote;
        }

        public void addDependency(int tid) {
            replayCauses.add(tid);
        }

        public HashSet<Integer> getReplayCauses() {
            return replayCauses;
        }
    }
}
