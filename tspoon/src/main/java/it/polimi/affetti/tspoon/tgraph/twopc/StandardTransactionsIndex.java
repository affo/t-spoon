package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by affo on 04/08/17.
 */
public class StandardTransactionsIndex extends TransactionsIndex {
    // I need a timestamp because a transaction can possibly be executed
    // more than once. However, I need to keep it separate from the transaction ID
    // (i.e. not overwrite it), because I want to track the dependencies among
    // transactions. I use the timestamp to check on the state operators if a
    // transaction has to be replayed or not; on the other hand, I use the transaction ID
    // to track the dependencies among transactions (a dependency is specified only if the
    // conflicting transaction has a timestamp greater than the last version saved).
    private AtomicInteger currentTimestamp = new AtomicInteger(0);
    private Map<Integer, Integer> timestampTidMapping = new HashMap<>();
    private Map<Integer, Set<Integer>> tidTimestampMapping = new HashMap<>();

    @Override
    public void updateWatermark(int tid, int timestamp, Vote vote) {
        timestamps.addInOrder(timestamp);
    }

    @Override
    public Set<Integer> getTimestamps(int tid) {
        return tidTimestampMapping.get(tid);
    }

    @Override
    public Integer newTimestamps(int tid) {
        return currentTimestamp.incrementAndGet();
    }

    @Override
    public Integer getTransactionId(int timestamp) {
        return timestampTidMapping.get(timestamp);
    }

    @Override
    public void addTransaction(int tid, int timestamp) {
        timestampTidMapping.put(timestamp, tid);
        tidTimestampMapping.computeIfAbsent(tid, k -> new HashSet<>()).add(timestamp);
    }

    @Override
    public void deleteTransaction(int tid) {
        Set<Integer> timestamps = tidTimestampMapping.remove(tid);
        timestamps.forEach(ts -> timestampTidMapping.remove(ts));
    }

    @Override
    public Tuple2<Long, Vote> getLogEntryFor(long timestamp, Vote vote) {
        return Tuple2.of(timestamp, vote);
    }
}
