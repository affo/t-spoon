package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * Provides results of executing transaction from a synchronous queue
 *
 * @param <R> The type of the future result to return
 */
public class PessimisticTransactionExecutor<R> implements
        KeyLevelTaskExecutor.TaskCompletionObserver<PessimisticTransactionExecutor<R>.OperationExecutionResult> {
    private KeyLevelTaskExecutor<OperationExecutionResult> executor;
    // taskId -> timestamp
    private Map<Long, Integer> taskTimestampMapping;
    // timestamp -> tasks
    private Map<Integer, List<Long>> timestampTaskMapping;
    // taskId -> futureResult
    private Map<Long, R> futureResults;
    private final BlockingQueue<OperationExecutionResult> resultQueue;

    public PessimisticTransactionExecutor(int numberOfExecutors) {
        this(numberOfExecutors, -1);
    }

    public PessimisticTransactionExecutor(int numberOfExecutors, long deadlockTimeout) {
        taskTimestampMapping = new HashMap<>();
        timestampTaskMapping = new HashMap<>();
        futureResults = new HashMap<>();

        resultQueue = new LinkedBlockingQueue<>();

        executor = new KeyLevelTaskExecutor<>(numberOfExecutors, this);
        if (deadlockTimeout > 0) {
            executor.enableDeadlockDetection(deadlockTimeout);
        }
        executor.startProcessing();
    }

    private synchronized void registerTask(long taskId, int timestamp, R futureResult) {
        taskTimestampMapping.put(taskId, timestamp);
        timestampTaskMapping.computeIfAbsent(timestamp, ts -> new LinkedList<>()).add(taskId);
        futureResults.put(taskId, futureResult);
    }

    private synchronized void unregisterTransaction(int timestamp) {
        List<Long> taskIds = timestampTaskMapping.remove(timestamp);
        taskIds.forEach(
                id -> {
                    taskTimestampMapping.remove(id);
                    futureResults.remove(id);
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
    public <V> void executeOperation(String key, Transaction<V> transaction, R futureResult) {
        Supplier<OperationExecutionResult> task = () -> {
            if (transaction.vote != Vote.COMMIT) {
                // no further processing for REPLAYed or ABORTed transactions
                return new OperationExecutionResult(transaction.vote, futureResult);
            }

            Object<V> object = transaction.getObject(key);
            ObjectVersion<V> version = object.getLastCommittedVersion();
            ObjectHandler<V> handler = version.createHandler();

            transaction.getOperation(key).accept(handler);
            Vote vote = handler.applyInvariant() ? Vote.COMMIT : Vote.ABORT;
            transaction.mergeVote(vote);
            object.addVersion(transaction.tid, transaction.timestamp, handler.object);

            OperationExecutionResult taskResult = new OperationExecutionResult(vote, futureResult);
            if (!handler.write) {
                taskResult.setReadOnly();
                transaction.setReadOnly(true);
            }

            return taskResult;
        };

        long id = executor.add(key, task);
        registerTask(id, transaction.timestamp, futureResult);
        executor.run(id);
    }

    public OperationExecutionResult getResult() throws InterruptedException {
        return resultQueue.take();
    }

    public <V> void onGlobalTermination(Transaction<V> transaction) {
        for (String key : transaction.getKeys()) {
            executor.free(key);
        }

        unregisterTransaction(transaction.timestamp);
    }

    @Override
    public void onTaskCompletion(long id, OperationExecutionResult taskResult) {
        resultQueue.add(taskResult);
    }

    @Override
    public void onDeadlock(LinkedHashMap<Long, List<Long>> deadlockedWithDependencies) {
        for (Map.Entry<Long, List<Long>> entry : deadlockedWithDependencies.entrySet()) {
            long id = entry.getKey();
            List<Long> dependencies = entry.getValue();

            OperationExecutionResult operationResult;
            synchronized (this) {
                R result = futureResults.get(id);
                operationResult = new OperationExecutionResult(Vote.REPLAY, result);
                for (Long dependency : dependencies) {
                    Integer timestamp = taskTimestampMapping.get(dependency);
                    if (timestamp == null) {
                        // It could happen that we detect a dependency with a task that run to completion
                        // and its transaction terminated globally.
                        // No matter if we detected a dependency with this transaction,
                        // we can skip tracking the dependency
                        continue;
                    }
                    operationResult.addDependency(timestamp);
                }
            }

            resultQueue.add(operationResult);
        }
    }

    public class OperationExecutionResult extends KeyLevelTaskExecutor.TaskResult {
        public final Vote vote;
        public final R futureResult;

        private final HashSet<Integer> replayCauses = new HashSet<>();

        private OperationExecutionResult(Vote vote, R futureResult) {
            this.vote = vote;
            this.futureResult = futureResult;
        }

        private void addDependency(int timestamp) {
            replayCauses.add(timestamp);
        }

        public HashSet<Integer> getReplayCauses() {
            return replayCauses;
        }
    }
}
