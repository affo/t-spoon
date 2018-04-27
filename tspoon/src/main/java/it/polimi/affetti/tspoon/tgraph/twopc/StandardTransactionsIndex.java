package it.polimi.affetti.tspoon.tgraph.twopc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by affo on 04/08/17.
 */
public class StandardTransactionsIndex<T> extends TransactionsIndex<T> {
    private Map<Long, Long> timestampTidMapping = new HashMap<>();
    private Map<Long, Set<Long>> tidTimestampMapping = new HashMap<>();

    public StandardTransactionsIndex(long startingPoint, int sourceParallelism, int sourceID) {
        super(startingPoint, sourceParallelism, sourceID);
    }

    @Override
    public Long getTransactionId(long timestamp) {
        return timestampTidMapping.get(timestamp);
    }

    @Override
    public LocalTransactionContext newTransaction(T element, long tid) {
        LocalTransactionContext transactionContext = super.newTransaction(element, tid);
        long timestamp = transactionContext.timestamp;
        timestampTidMapping.put(timestamp, tid);
        tidTimestampMapping.computeIfAbsent(tid, k -> new HashSet<>()).add(timestamp);
        return transactionContext;
    }

    @Override
    public void deleteTransaction(long tid) {
        super.deleteTransaction(tid);
        Set<Long> timestamps = tidTimestampMapping.remove(tid);
        timestamps.forEach(ts -> timestampTidMapping.remove(ts));
    }
}
