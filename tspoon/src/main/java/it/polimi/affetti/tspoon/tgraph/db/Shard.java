package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.query.PredicateQuery;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import it.polimi.affetti.tspoon.tgraph.query.QueryVisitor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private final Map<Integer, Transaction<V>> transactions;
    private Object.DeferredReadListener deferredReadListener;

    public Shard(
            String nameSpace,
            int shardNumber,
            int numberOfShards,
            int maxNumberOfVersions,
            boolean externalReadCommitted,
            ObjectFunction<V> objectFunction) {
        this.nameSpace = nameSpace;
        this.shardNumber = shardNumber;
        this.numberOfShards = numberOfShards;
        this.externalReadCommitted = externalReadCommitted;
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
        query.accept(this);
        return query.getResult();
    }

    public boolean transactionExist(int timestamp) {
        return transactions.containsKey(timestamp);
    }

    public Transaction<V> getTransaction(int timestamp) {
        return transactions.get(timestamp);
    }

    public Transaction<V> removeTransaction(int timestamp) {
        return transactions.remove(timestamp);
    }

    // --------------------------------------- Querying ---------------------------------------

    private V queryGetsObject(String key, int wm) {
        synchronized (state) {
            Object<V> object = state.get(key);
            if (object == null) {
                return null;
            }

            if (externalReadCommitted) {
                return object.readCommittedBefore(wm).object;
            }

            return object.getLastAvailableVersion().object;
        }
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
}
