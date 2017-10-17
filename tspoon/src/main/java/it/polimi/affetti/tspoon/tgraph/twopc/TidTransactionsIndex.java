package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collections;
import java.util.Set;

/**
 * Created by affo on 04/08/17.
 */
public class TidTransactionsIndex extends TransactionsIndex {
    @Override
    public void updateWatermark(int tid, int timestamp, Vote vote) {
        if (vote != Vote.REPLAY) {
            timestamps.addInOrder(tid);
        }
    }

    @Override
    public Set<Integer> getTimestamps(int tid) {
        return Collections.singleton(tid);
    }

    @Override
    public Integer newTimestamps(int tid) {
        return tid;
    }

    @Override
    public Integer getTransactionId(int timestamp) {
        return timestamp;
    }

    @Override
    public void addTransaction(int tid, int timestamp) {
        // does nothing
    }

    @Override
    public void deleteTransaction(int tid) {
        // does nothing
    }

    @Override
    public Tuple2<Long, Vote> getLogEntryFor(long timestamp, Vote vote) {
        if (vote == Vote.REPLAY) {
            return null;
        }

        return Tuple2.of(timestamp, vote);
    }
}
