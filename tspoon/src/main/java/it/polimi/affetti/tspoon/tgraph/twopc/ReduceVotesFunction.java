package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.BatchCompletionChecker;
import it.polimi.affetti.tspoon.tgraph.BatchID;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 29/01/17.
 */
public class ReduceVotesFunction extends RichFlatMapFunction<Metadata, Metadata> {
    private Map<Integer, Metadata> votes = new HashMap<>();
    private transient BatchCompletionChecker completionChecker;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        completionChecker = new BatchCompletionChecker();
    }

    @Override
    public void flatMap(Metadata metadata, Collector<Metadata> collector) throws Exception {
        int timestamp = metadata.timestamp;

        Metadata accumulated = votes.get(timestamp);
        if (accumulated == null) {
            // first one
            accumulated = metadata.clone(new BatchID(metadata.tid));
        } else {
            accumulated.vote = accumulated.vote.merge(metadata.vote);
            accumulated.cohorts.addAll(metadata.cohorts);
            accumulated.dependencyTracking.addAll(metadata.dependencyTracking);
        }
        votes.put(timestamp, accumulated);

        if (completionChecker.checkCompleteness(timestamp, metadata.batchID)) {
            completionChecker.freeIndex(timestamp);
            collector.collect(votes.remove(timestamp));
        }
    }
}
