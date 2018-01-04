package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.OrderedElements;
import it.polimi.affetti.tspoon.common.OrderedTimestamps;
import it.polimi.affetti.tspoon.common.SimpleOrderedElements;

import java.util.*;

/**
 * Created by affo on 28/12/17.
 */
public class TotalOrderEnforcer {
    private long lastRemoved = 0;
    private BatchCompletionChecker completionChecker = new BatchCompletionChecker();
    private OrderedTimestamps timestamps = new OrderedTimestamps();
    private Map<Integer, SimpleOrderedElements<BatchID>> bids = new HashMap<>();

    public void addElement(int timestamp, BatchID bid) {
        timestamps.addInOrderWithoutRepetition(timestamp);
        bids.computeIfAbsent(timestamp, ts -> new SimpleOrderedElements<>()).addInOrder(bid);
        completionChecker.checkCompleteness(timestamp, bid);
    }

    public LinkedHashMap<Integer, List<BatchID>> next() {
        LinkedHashMap<Integer, List<BatchID>> result = new LinkedHashMap<>();

        final long[] newLastRemoved = {lastRemoved};
        timestamps.peekContiguous(lastRemoved,
                timestamp -> {
                    if (!completionChecker.getCompleteness(timestamp)) {
                        return Collections.singleton(OrderedElements.IterationAction.STOP_ITERATION);
                    }

                    completionChecker.freeIndex(timestamp);
                    SimpleOrderedElements<BatchID> completeBid = bids.remove(timestamp);
                    result.put(timestamp, completeBid.toList());
                    newLastRemoved[0] = timestamp;

                    return Collections.singleton(OrderedElements.IterationAction.REMOVE_FROM_ELEMENTS);
                });

        lastRemoved = newLastRemoved[0];
        return result;
    }

    public int getNumberOfIncompleteElements() {
        return timestamps.size();
    }

    public int getTotalNumberOfIncompleteElements() {
        return bids.values().stream()
                .mapToInt(SimpleOrderedElements::size).sum();
    }
}
