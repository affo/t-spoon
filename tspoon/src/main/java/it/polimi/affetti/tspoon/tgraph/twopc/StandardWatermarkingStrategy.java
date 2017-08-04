package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;

/**
 * Created by affo on 04/08/17.
 */
public class StandardWatermarkingStrategy extends WatermarkingStrategy {
    @Override
    public void notifyTermination(int tid, int timestamp, Vote vote) {
        timestamps.addInOrder(timestamp);
    }
}
