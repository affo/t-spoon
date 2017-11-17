package it.polimi.affetti.tspoon.tgraph.twopc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by affo on 04/08/17.
 */
public class StandardTransactionsIndex<T> extends TransactionsIndex<T> {
    private Map<Integer, Integer> timestampTidMapping = new HashMap<>();
    private Map<Integer, Set<Integer>> tidTimestampMapping = new HashMap<>();

    @Override
    public Integer getTransactionId(int timestamp) {
        return timestampTidMapping.get(timestamp);
    }

    @Override
    public LocalTransactionContext newTransaction(T element, int tid) {
        LocalTransactionContext transactionContext = super.newTransaction(element, tid);
        int timestamp = transactionContext.timestamp;
        timestampTidMapping.put(timestamp, tid);
        tidTimestampMapping.computeIfAbsent(tid, k -> new HashSet<>()).add(timestamp);
        return transactionContext;
    }

    @Override
    public void deleteTransaction(int tid) {
        super.deleteTransaction(tid);
        Set<Integer> timestamps = tidTimestampMapping.remove(tid);
        timestamps.forEach(ts -> timestampTidMapping.remove(ts));
    }
}
