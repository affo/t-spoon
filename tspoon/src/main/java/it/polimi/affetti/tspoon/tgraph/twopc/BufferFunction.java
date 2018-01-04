package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.BatchCompletionChecker;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 29/01/17.
 */
public class BufferFunction<T> extends
        RichCoFlatMapFunction<Enriched<T>, Metadata, Enriched<T>> {
    private Map<Integer, List<Enriched<T>>> batches = new HashMap<>();
    private Map<Integer, Metadata> votes = new HashMap<>();
    private transient BatchCompletionChecker completionChecker;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        completionChecker = new BatchCompletionChecker();
    }

    private void addElement(int timestamp, Enriched<T> element) {
        batches.computeIfAbsent(timestamp, k -> new LinkedList<>()).add(element);
    }

    @Override
    public void flatMap1(Enriched<T> element, Collector<Enriched<T>> collector) throws Exception {
        int timestamp = element.metadata.timestamp;

        Metadata vote = votes.get(timestamp);
        boolean complete = completionChecker.checkCompleteness(timestamp, element.metadata.batchID);

        if (vote != null) {
            element.metadata.vote = vote.vote;
            collector.collect(element);

            if (complete) {
                votes.remove(timestamp);
                completionChecker.freeIndex(timestamp);
            }
        } else {
            addElement(timestamp, element);
        }
    }

    @Override
    public void flatMap2(Metadata metadata, Collector<Enriched<T>> collector) throws Exception {
        int timestamp = metadata.timestamp;
        votes.put(timestamp, metadata);

        List<Enriched<T>> batch = batches.remove(timestamp);

        if (batch == null) {
            return;
        }

        for (Enriched<T> element : batch) {
            element.metadata.vote = metadata.vote;
            collector.collect(element);
        }

        if (completionChecker.getCompleteness(timestamp)) {
            votes.remove(timestamp);
            completionChecker.freeIndex(timestamp);
        }
    }
}
