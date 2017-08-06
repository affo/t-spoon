package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedTimestamps;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.Collections;
import java.util.List;

/**
 * Created by affo on 04/08/17.
 */
public abstract class WatermarkingStrategy {
    protected final OrderedTimestamps timestamps = new OrderedTimestamps();
    private int lastRemoved = 0;

    public abstract void notifyTermination(int tid, int timestamp, Vote vote);

    Integer getLastCompleted() {
        synchronized (timestamps) {
            List<Integer> removed = timestamps.removeContiguousWith(lastRemoved);

            if (removed.isEmpty()) {
                return null;
            }

            int result = Collections.max(removed);

            lastRemoved = result;
            return result;
        }
    }
}
