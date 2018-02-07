package it.polimi.affetti.tspoon.tgraph.db;

import it.polimi.affetti.tspoon.common.RandomProvider;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.query.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    // --------------------------------------- Transaction Execution and Completion ---------------------------------------

    protected synchronized Object<V> getObject(String key) {
        return state.computeIfAbsent(key, k -> {
            keySpaceIndex.add(k);
            return new Object<>(nameSpace, k, objectFunction);
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

    private void queryState(Query query) {
        QueryResult queryResult = query.getResult();
        for (String key : query.keys) {
            V object;
            if (externalReadCommitted) {
                object = getObject(key).readCommittedBefore(query.watermark).object;
            } else {
                object = getObject(key).getLastAvailableVersion().object;
            }

            if (object != null) {
                queryResult.add(key, object);
            }
        }
    }

    // randomizer to build queries
    private Random random = RandomProvider.get();

    @Override
    public void visit(Query query) {
        queryState(query);
    }

    @Override
    public void visit(RandomQuery query) {
        int noKeys = state.size();
        int noKeysToQuery = query.size / numberOfShards;

        if (shardNumber == 0) {
            // if you are the first shard, get the remaining keys
            noKeysToQuery += state.size() % numberOfShards;
        }

        if (state.isEmpty()) {
            return;
        }

        Stream<Integer> indexes;
        if (noKeys > noKeysToQuery) {
            indexes = random.ints(0, noKeys).distinct().limit(noKeysToQuery)
                    .boxed();
        } else {
            // select *
            indexes = IntStream.range(0, keySpaceIndex.size()).boxed();
        }

        indexes.forEach(index -> query.addKey(keySpaceIndex.get(index)));
        queryState(query);
    }

    @Override
    public <T> void visit(PredicateQuery<T> query) {
        QueryResult result = query.getResult();

        for (String key : state.keySet()) {
            V value = state.get(key).getLastVersionBefore(query.watermark).object;
            // hope that the predicate is coherent with the state
            try {
                if (query.test((T) value)) {
                    result.add(key, value);
                }
            } catch (ClassCastException e) {
                LOG.error("Problem with provided predicate: " + e.getMessage());
                return;
            }
        }
    }
}
