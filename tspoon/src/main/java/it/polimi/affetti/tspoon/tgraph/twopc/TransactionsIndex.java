package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedTimestamps;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Created by affo on 04/08/17.
 */
public abstract class TransactionsIndex {
    protected final OrderedTimestamps timestamps = new OrderedTimestamps();
    protected int lastRemoved = 0;

    public abstract void updateWatermark(int tid, int timestamp, Vote vote);

    public abstract Set<Integer> getTimestamps(int tid);

    public abstract Integer newTimestamps(int tid);

    public abstract Integer getTransactionId(int timestamp);

    public abstract void addTransaction(int tid, int timestamp);

    public abstract void deleteTransaction(int tid);

    public abstract Tuple2<Long, Vote> getLogEntryFor(long timestamp, Vote vote);

    Integer getLastCompleted() {
        List<Integer> removed = timestamps.removeContiguousWith(lastRemoved);

        if (removed.isEmpty()) {
            return null;
        }

        int result = Collections.max(removed);

        lastRemoved = result;
        return result;
    }
}
