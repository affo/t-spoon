package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;

/**
 * Created by affo on 04/08/17.
 */
public class TidTransactionsIndex extends TransactionsIndex {
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
    public LocalTransactionContext newTransaction(int tid) {
        LocalTransactionContext localTransactionContext = new LocalTransactionContext();
        localTransactionContext.tid = tid;
        localTransactionContext.timestamp = tid; // same tid <-> ts
        executions.put(tid, localTransactionContext);
        return localTransactionContext;
    }
}
