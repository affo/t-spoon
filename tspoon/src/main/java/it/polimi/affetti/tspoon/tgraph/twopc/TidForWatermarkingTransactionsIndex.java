package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedElements;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.Collections;
import java.util.List;

/**
 * Created by affo on 04/08/17.
 *
 * Uses transaction ids as watermarks.
 *
 * At (optimistic) PL4 isolation level, we use transaction ids for versioning.
 */
public class TidForWatermarkingTransactionsIndex<T> extends StandardTransactionsIndex<T> {
    private OrderedElements<Long> watermarks = new OrderedElements<>(wm -> wm);
    private long watermark;

    public TidForWatermarkingTransactionsIndex(long startingPoint, int sourceParallelism, int sourceID) {
        super(startingPoint, sourceParallelism, sourceID);
        this.watermark = startingPoint;
    }

    @Override
    public long updateWatermark(long timestamp, Vote vote) {
        if (vote != Vote.REPLAY) {
            // no update if the vote is replay, because the tnx has not yet completed
            long tid = getTransactionId(timestamp);
            watermarks.addInOrder(tid);
            List<Long> removed = watermarks.removeContiguousWith(watermark);

            if (!removed.isEmpty()) {
                watermark = Collections.max(removed);
            }
        }

        return watermark;
    }

    @Override
    public long getCurrentWatermark() {
        return watermark;
    }
}
