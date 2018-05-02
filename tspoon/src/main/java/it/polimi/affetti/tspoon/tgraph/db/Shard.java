package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.RPC;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.query.PredicateQuery;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import it.polimi.affetti.tspoon.tgraph.query.QueryVisitor;
import it.polimi.affetti.tspoon.tgraph.state.SinglePartitionUpdate;
import it.polimi.affetti.tspoon.tgraph.twopc.WAL;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created by affo on 19/12/17.
 */
public class Shard<V> implements
        QueryVisitor {
    private Logger LOG;
    protected final String nameSpace;
    // I suppose that the type for keys is String. This assumption is coherent,
    // for instance, with Redis implementation: https://redis.io/topics/data-types-intro
    protected final Map<String, Object<V>> state;
    protected final List<String> keySpaceIndex;
    private final int shardNumber, numberOfShards;
    private final boolean externalReadCommitted;
    private final ObjectFunction<V> objectFunction;
    // transaction contexts: timestamp -> context
    private final Map<Long, Transaction<V>> transactions;
    private Object.DeferredReadListener deferredReadListener;

    private final transient WAL wal;
    private final Semaphore recoverySemaphore = new Semaphore(0);

    public Shard(
            String nameSpace,
            int shardNumber,
            int numberOfShards,
            int maxNumberOfVersions,
            boolean externalReadCommitted,
            WAL wal,
            ObjectFunction<V> objectFunction) {
        this.nameSpace = nameSpace;
        this.shardNumber = shardNumber;
        this.numberOfShards = numberOfShards;
        this.externalReadCommitted = externalReadCommitted;
        this.wal = wal;
        this.objectFunction = objectFunction;
        Object.maxNumberOfVersions = maxNumberOfVersions;

        this.LOG = Logger.getLogger("Shard[" + shardNumber + "] - " + nameSpace);
        this.state = new ConcurrentHashMap<>();
        this.transactions = new ConcurrentHashMap<>();
        this.keySpaceIndex = new ArrayList<>();
    }

    public void forceSerializableRead() {
        Object.forceSerializableRead();
    }

    public void setDeferredReadsListener(Object.DeferredReadListener listener) {
        this.deferredReadListener = listener;
    }

    // --------------------------------------- Transaction Execution and Completion ---------------------------------------

    protected synchronized Object<V> getObject(String key) {
        return state.computeIfAbsent(key, k -> {
            keySpaceIndex.add(k);
            Object<V> object = new Object<>(nameSpace, k, objectFunction);
            if (deferredReadListener != null) {
                object.listenToDeferredReads(deferredReadListener);
            }
            return object;
        });
    }

    /**
     * Returns true if it creates a new Transaction
     */
    public boolean addOperation(
            String key, Metadata metadata, Operation<V> operation) throws Exception {
        waitForRecoveryCompletion(); // cannot apply operations if the shard is recovering

        Object<V> object = getObject(key);

        final boolean[] newTransaction = {false};
        Transaction<V> transaction = transactions.computeIfAbsent(metadata.timestamp,
                ts -> {
                    newTransaction[0] = true;
                    return new Transaction<>(metadata.tid, ts, metadata.watermark, metadata.coordinator);
                });
        transaction.mergeVote(metadata.vote);
        transaction.addDependencies(metadata.dependencyTracking);
        transaction.addOperation(key, object, operation);

        return newTransaction[0];
    }

    public QueryResult runQuery(Query query) {
        waitForRecoveryCompletion(); // cannot query if the shard is recovering

        query.accept(this);
        return query.getResult();
    }

    public TransactionResult runSinglePartitionUpdate(SinglePartitionUpdate update) throws
            InvocationTargetException, IllegalAccessException {
        waitForRecoveryCompletion(); // cannot apply operations if the shard is recovering

        String key = update.getKey();
        Object<V> object = getObject(key);

        object.lock(true);
        try {
            ObjectHandler<V> handler = object.readLastCommittedVersion();
            long tid = -1; // single-partition updated have no transaction id
            // TODO represent sub-versioning in versions (for durability)
            long version = handler.version; // append the version as a new version of the last timestamp

            RPC rpc = update.getCommand();
            V read = handler.read();
            rpc.addParam(read);
            // hope to get the right command for the right state (as it is for predicate queries)
            V written = (V) rpc.call(objectFunction.getStateFunction());
            if (written != read) {
                handler.write(written);
            }

            Vote vote = handler.applyInvariant() ? Vote.COMMIT : Vote.ABORT;
            Updates updates = new Updates();

            if (vote == Vote.COMMIT) {
                updates.addUpdate(nameSpace, key, handler.object);
                wal.addEntry(new WAL.Entry(vote, tid, version, updates));
                object.installVersion(tid, version, handler.object);
            }

            return new TransactionResult(tid, version, update, vote, updates);
        } catch (IOException e) {
            throw new RuntimeException("Problem in adding an SPU entry to the WAL: " + e.getMessage());
        } finally {
            object.unlock();
        }
    }

    public boolean transactionExist(long timestamp) {
        return transactions.containsKey(timestamp);
    }

    public Transaction<V> getTransaction(long timestamp) {
        return transactions.get(timestamp);
    }

    public Transaction<V> removeTransaction(long timestamp) {
        return transactions.remove(timestamp);
    }

    // --------------------------------------- Querying ---------------------------------------

    private V queryGetsObject(String key, long wm) {
        Object<V> object;
        synchronized (state) {
            object = state.get(key);
            if (object == null) {
                return null;
            }
        }

        if (externalReadCommitted) {
            return object.readCommittedBefore(wm).object;
        }

        return object.getLastAvailableVersion().object;
    }

    private void queryState(Query query) {
        QueryResult queryResult = query.getResult();
        for (String key : query.keys) {
            V object = queryGetsObject(key, query.watermark);
            if (object != null) {
                queryResult.add(key, object);
            }
        }
    }

    @Override
    public void visit(Query query) {
        queryState(query);
    }

    @Override
    public <T> void visit(PredicateQuery<T> query) {
        QueryResult result = query.getResult();

        for (String key : state.keySet()) {
            V object = queryGetsObject(key, query.watermark);
            // hope that the predicate is coherent with the state
            try {
                if (query.test((T) object)) {
                    result.add(key, object);
                }
            } catch (ClassCastException e) {
                LOG.error("Problem with provided predicate: " + e.getMessage());
                return;
            }
        }
    }

    // --------------------------------------- Recovery & Snapshotting ---------------------------------------

    public Map<String, V> getConsistentSnapshot(long wm) {
        Map<String, V> snapshot = new HashMap<>();

        for (Map.Entry<String, Object<V>> entry : state.entrySet()) {
            String key = entry.getKey();
            Object<V> object = entry.getValue();
            V value = object.getLastVersionBefore(wm).object;
            snapshot.put(key, value);
        }
        return snapshot;
    }

    public void installSnapshot(Map<String, V> snapshot) {
        for (Map.Entry<String, V> entry : snapshot.entrySet()) {
            String key = entry.getKey();
            V value = entry.getValue();
            Object<V> object = getObject(key);
            object.addVersion(-1, 0, value);
        }
    }

    public void recover(String key, V value) {
        boolean green = recoverySemaphore.tryAcquire();

        if (green) {
            throw new IllegalStateException("Cannot recover if not in recovery mode");
        }

        Object<V> object = getObject(key);
        object.addVersion(-1, 0, value);
    }

    public void signalRecoveryComplete() {
        recoverySemaphore.release();
    }

    private void waitForRecoveryCompletion() {
        recoverySemaphore.acquireUninterruptibly();
        recoverySemaphore.release();
    }
}
