package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 29/01/17.
 */
public class ReduceVotesFunction implements FlatMapFunction<Metadata, Metadata> {
    private Map<Integer, Metadata> votes = new HashMap<>();
    private Map<Integer, Integer> counts = new HashMap<>();

    private int incrementCounter(int timestamp) {
        int count = counts.getOrDefault(timestamp, 0);
        count++;
        counts.put(timestamp, count);
        return count;
    }

    @Override
    public void flatMap(Metadata metadata, Collector<Metadata> collector) throws Exception {
        int timestamp = metadata.timestamp;

        Metadata accumulated = votes.get(timestamp);
        if (accumulated == null) {
            // first one
            accumulated = metadata;
        } else {
            accumulated.vote = accumulated.vote.merge(metadata.vote);
            accumulated.cohorts.addAll(metadata.cohorts);
            accumulated.dependencyTracking.addAll(metadata.dependencyTracking);
        }
        votes.put(timestamp, accumulated);

        if (incrementCounter(timestamp) >= metadata.batchSize) {
            counts.remove(timestamp);
            collector.collect(votes.remove(timestamp));
        }
    }
}
