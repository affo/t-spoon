package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Vote;

/**
 * Created by affo on 04/08/17.
 */
public class TidWatermarkingStrategy extends WatermarkingStrategy {
    @Override
    public void notifyTermination(int tid, int timestamp, Vote vote) {
        if (vote != Vote.REPLAY) {
            timestamps.addInOrder(tid);
        }
    }
}
