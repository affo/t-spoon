package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 29/01/17.
 */
public class BufferRecordFunction<T> implements
        CoFlatMapFunction<Enriched<T>, Metadata, Enriched<T>> {
    private Map<Integer, List<Enriched<T>>> batches = new HashMap<>();
    private Map<Integer, Metadata> votes = new HashMap<>();
    private Map<Integer, Integer> counts = new HashMap<>();

    private void addElement(int timestamp, Enriched<T> element) {
        int size = element.metadata.batchSize;
        batches.computeIfAbsent(timestamp, k -> new ArrayList<>(size)).add(element);
    }

    private int incrementCounter(int timestamp) {
        int count = counts.getOrDefault(timestamp, 0);
        count++;
        counts.put(timestamp, count);
        return count;
    }

    @Override
    public void flatMap1(Enriched<T> element, Collector<Enriched<T>> collector) throws Exception {
        int timestamp = element.metadata.timestamp;

        Metadata vote = votes.get(timestamp);
        int count = incrementCounter(timestamp);

        if (vote != null) {
            element.metadata.vote = vote.vote;
            collector.collect(element);

            if (count >= element.metadata.batchSize) {
                votes.remove(timestamp);
                counts.remove(timestamp);
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

        int batchSize = batch.get(0).metadata.batchSize;

        for (Enriched<T> element : batch) {
            element.metadata.vote = metadata.vote;
            collector.collect(element);
        }

        if (batch.size() >= batchSize) {
            votes.remove(timestamp);
            counts.remove(timestamp);
        }
    }
}
