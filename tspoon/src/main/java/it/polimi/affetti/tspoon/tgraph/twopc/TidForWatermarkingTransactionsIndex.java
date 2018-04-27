package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;

/**
 * Created by affo on 04/08/17.
 *
 * Uses transaction ids as watermarks.
 *
 * At (optimistic) PL4 isolation level, we use transaction ids for versioning.
 */
public class TidForWatermarkingTransactionsIndex<T> extends StandardTransactionsIndex<T> {
    public TidForWatermarkingTransactionsIndex(long startingPoint, int sourceParallelism, int sourceID) {
        super(startingPoint, sourceParallelism, sourceID);
    }

    @Override
    public long updateWatermark(long timestamp, Vote vote) {
        long tid = getTransactionId(timestamp);
        if (vote != Vote.REPLAY) {
            // we use transaction ids for the watermark
            return super.updateWatermark(tid, vote);
        }

        // no update
        return getCurrentWatermark();
    }
}
