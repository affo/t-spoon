package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;

/**
 * Created by affo on 04/08/17.
 */
public class TidTransactionsIndex<T> extends TransactionsIndex<T> {
    @Override
    public int updateWatermark(int timestamp, Vote vote) {
        if (vote != Vote.REPLAY) {
            return super.updateWatermark(timestamp, vote);
        }

        // no update
        return getCurrentWatermark();
    }

    @Override
    public Integer getTransactionId(int timestamp) {
        return timestamp;
    }

    @Override
    // total override, no super()
    public LocalTransactionContext newTransaction(T element, int tid) {
        LocalTransactionContext localTransactionContext = new LocalTransactionContext();
        localTransactionContext.tid = tid;
        localTransactionContext.timestamp = tid; // same tid <-> ts
        localTransactionContext.element = element;
        executions.put(tid, localTransactionContext);
        return localTransactionContext;
    }
}
