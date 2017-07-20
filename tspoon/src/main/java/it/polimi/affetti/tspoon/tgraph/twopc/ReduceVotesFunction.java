package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.common.Address;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 29/01/17.
 */
public class ReduceVotesFunction implements FlatMapFunction<TwoPCData, TwoPCData> {
    private Map<Integer, TwoPCData> votes = new HashMap<>();
    private Map<Integer, Integer> counts = new HashMap<>();
    private Map<Integer, List<Address>> cohortsByTID = new HashMap<>();

    private int incrementCounter(int tid) {
        int count = counts.getOrDefault(tid, 0);
        count++;
        counts.put(tid, count);
        return count;
    }

    private void updateCohorts(int tid, List<Address> newCohorts) {
        cohortsByTID.putIfAbsent(tid, new LinkedList<>());
        cohortsByTID.get(tid).addAll(newCohorts);
    }

    @Override
    public void flatMap(TwoPCData twoPCData, Collector<TwoPCData> collector) throws Exception {
        int tid = twoPCData.tid;
        updateCohorts(tid, twoPCData.cohorts);

        TwoPCData accumulated = votes.get(tid);
        if (accumulated == null) {
            accumulated = twoPCData;
        } else {
            accumulated.vote = accumulated.vote.merge(twoPCData.vote);
        }
        votes.put(tid, accumulated);

        if (incrementCounter(tid) >= twoPCData.batchSize) {
            counts.remove(tid);
            cohortsByTID.remove(tid);
            collector.collect(votes.remove(tid));
        }
    }
}
