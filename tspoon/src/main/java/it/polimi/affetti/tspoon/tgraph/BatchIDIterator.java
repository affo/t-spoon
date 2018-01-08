package it.polimi.affetti.tspoon.tgraph;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Iterator;

/**
 * Created by affo on 05/01/18.
 */
public class BatchIDIterator implements Iterator<Tuple2<Integer, Integer>> {
    private final BatchID bid;
    private Iterator<Integer> offsetsIterator, sizesIterator;
    private boolean hasNext;

    public BatchIDIterator(BatchID batchID) {
        this.bid = batchID;
        offsetsIterator = bid.offsets.iterator();
        sizesIterator = bid.sizes.iterator();
        hasNext = offsetsIterator.hasNext();
    }

    @Override
    public boolean hasNext() {
        if (!offsetsIterator.hasNext() && bid.newOffset == 0) {
            // in a consolidated batch ID
            return false;
        }

        return hasNext;
    }

    @Override
    public Tuple2<Integer, Integer> next() {
        int offset, size;
        if (!offsetsIterator.hasNext()) {
            offset = bid.newOffset;
            size = bid.newSize;
            hasNext = false;
        } else {
            offset = offsetsIterator.next();
            size = sizesIterator.next();
        }

        return Tuple2.of(offset, size);
    }
}
