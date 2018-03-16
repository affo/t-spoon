package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Provides results of executing transaction from a synchronous queue.
 */
public class PessimisticTransactionExecutor implements
        KeyLevelTaskExecutor.TaskCompletionObserver<Void> {
    private KeyLevelTaskExecutor<Void> executor;
    private Map<Integer, Integer> timestampTidMapping = new ConcurrentHashMap<>();

    public PessimisticTransactionExecutor(int numberOfExecutors) {
        executor = new KeyLevelTaskExecutor<>(numberOfExecutors, this);
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
     *
     * In order to preserve a global order for versioning, we need to REPLAY transactions that happen out of order.
     * Indeed, you have no guarantee that, if T_i reads version j and writes version i, then i > j!
     * The last committed version would be j, even if i committed and it read j.
     * This could lead another transaction (read-only or not) to get an inconsistent state.
     * For instance, take objects X and Y and (tid, ts) pairs for versions. The figure represents the order of version
     * creation through time:
     *
     * X: <-- (6, 6) -- (4, 4) -- (5, 5) -- t
     * Y: <-- (4, 4) -- t
     *
     * The last committed version for X is 5, while for Y it is 4.
     * This causes a LOST UPDATE anomaly at X: (6, 6) accesses version 5, indeed.
     * If somebody reads from the outside, it cannot trust the timestamps anymore!
     *
     * In order to prevent this kind of anomaly, we reorder transaction execution if possible, otherwise
     * we REPLAY transactions (see #KeyLevelTaskExecutor for more information).
     *
     * At PL4, transaction ordering matches the timestamp, so there is no problem with external querying.
     */
    public <V> void executeOperation(String key, Transaction<V> transaction, Callback callback) {
        Supplier<Void> task = () -> {
            // no further processing for REPLAYed or ABORTed transactions
            if (transaction.vote == Vote.COMMIT) {
                Object<V> object = transaction.getObject(key);
                object.lock(false);
                try {
                    ObjectHandler<V> handler = object.readLastCommittedVersion();

                    transaction.getOperation(key).accept(handler);
                    Vote vote = handler.applyInvariant() ? Vote.COMMIT : Vote.ABORT;
                    transaction.mergeVote(vote);
                    ObjectVersion<V> objectVersion = object.addVersion(transaction.tid, transaction.timestamp, handler.object);
                    transaction.addVersion(key, objectVersion);

                    if (!handler.write) {
                        transaction.setReadOnly(true);
                    }
                } finally {
                    object.unlock();
                }
            }

            callback.execute();
            return null;
        };

        try {
            timestampTidMapping.put(transaction.timestamp, transaction.tid);
            executor.run(transaction.timestamp, key, task);
        } catch (KeyLevelTaskExecutor.OutOfOrderTaskException e) {
            transaction.mergeVote(Vote.REPLAY);
            Integer replayCause = timestampTidMapping.get((int) e.getGreaterID());
            if (replayCause != null) {
                // the replayCause could be `null` in the case its transaction has already terminated globally.
                // in this case it is useless to specify it as a dependency (it has already finished...)
                transaction.addDependency(replayCause);
            }
            callback.execute();
        }
    }

    public <V> void onGlobalTermination(Transaction<V> transaction) {
        for (String key : transaction.getKeys()) {
            try {
                executor.complete(key, transaction.timestamp);
            } catch (IllegalStateException ise) {
                if (transaction.vote != Vote.REPLAY) {
                    throw ise;
                }

                // DO NOTHING...
                // This means that the completed transaction had been REPLAYed
                // and it is totally normal that its task hadn't been registered at the executor
            }
        }

        timestampTidMapping.remove(transaction.timestamp);
    }

    @Override
    public void onTaskCompletion(String key, long id, Void useless) {
        // does nothing
    }

    @FunctionalInterface
    public interface Callback {
        void execute();
    }
}
