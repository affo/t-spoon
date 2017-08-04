package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.OrderedTimestamps;
import it.polimi.affetti.tspoon.tgraph.Vote;

import java.util.Collections;

/**
 * Created by affo on 04/08/17.
 */
public abstract class WatermarkingStrategy {
    protected OrderedTimestamps timestamps = new OrderedTimestamps();
    private int lastRemoved = 0;

    public abstract void notifyTermination(int tid, int timestamp, Vote vote);

    Integer getLastCompleted() {
        timestamps.addInOrder(lastRemoved);
        int result = Collections.max(timestamps.removeContiguous());
        if (result == lastRemoved) {
            return null;
        }
        lastRemoved = result;

        return result;
    }
}
