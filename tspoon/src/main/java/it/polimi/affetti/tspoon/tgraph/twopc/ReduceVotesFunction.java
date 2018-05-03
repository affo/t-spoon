package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.BatchCompletionChecker;
import it.polimi.affetti.tspoon.tgraph.BatchID;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 29/01/17.
 */
public class ReduceVotesFunction extends RichFlatMapFunction<Metadata, Metadata> {
    private Map<Long, Metadata> votes = new HashMap<>();
    private transient BatchCompletionChecker completionChecker;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        completionChecker = new BatchCompletionChecker();
    }

    @Override
    public void flatMap(Metadata metadata, Collector<Metadata> collector) throws Exception {
        long timestamp = metadata.timestamp;

        BatchID batchIDToCheck = metadata.batchID;
        Metadata accumulated = votes.get(timestamp);
        if (accumulated == null) {
            // first one:
            // use the original metadata, but refresh the batch id for further reduction
            metadata.batchID = new BatchID(metadata.tid);
            accumulated = metadata;
        } else {
            accumulated.vote = accumulated.vote.merge(metadata.vote);
            accumulated.cohorts.addAll(metadata.cohorts);
            accumulated.dependencyTracking.addAll(metadata.dependencyTracking);

            if (accumulated.vote == Vote.COMMIT) {
                accumulated.mergeUpdates(metadata.updates);
            } else {
                accumulated.updates.clear();
            }
        }
        votes.put(timestamp, accumulated);

        if (completionChecker.checkCompleteness(timestamp, batchIDToCheck)) {
            completionChecker.freeIndex(timestamp);
            collector.collect(votes.remove(timestamp));
        }
    }
}
