package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Enriched;
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
        CoFlatMapFunction<Enriched<T>, TwoPCData, Enriched<T>> {
    private Map<Integer, List<Enriched<T>>> batches = new HashMap<>();
    private Map<Integer, TwoPCData> votes = new HashMap<>();
    private Map<Integer, Integer> counts = new HashMap<>();

    private List<Enriched<T>> getOrCreateBatch(int tid, int size) {
        batches.putIfAbsent(tid, new ArrayList<>(size));
        return batches.get(tid);
    }

    private void addElement(int tid, Enriched<T> element) {
        getOrCreateBatch(tid, element.tContext().twoPC.batchSize).add(element);
    }

    private int incrementCounter(int tid) {
        int count = counts.getOrDefault(tid, 0);
        count++;
        counts.put(tid, count);
        return count;
    }

    @Override
    public void flatMap1(Enriched<T> element, Collector<Enriched<T>> collector) throws Exception {
        int tid = element.tContext().getTid();

        TwoPCData vote = votes.get(tid);
        int count = incrementCounter(tid);

        if (vote != null) {
            if (vote.isCollectable()) {
                collector.collect(element);
            }

            if (count >= element.tContext().twoPC.batchSize) {
                votes.remove(tid);
                counts.remove(tid);
            }
        } else {
            addElement(tid, element);
        }
    }

    @Override
    public void flatMap2(TwoPCData twoPCData, Collector<Enriched<T>> collector) throws Exception {
        int tid = twoPCData.tid;
        votes.put(tid, twoPCData);

        List<Enriched<T>> batch = batches.remove(tid);

        if (batch == null) {
            return;
        }

        int batchSize = batch.get(0).tContext().twoPC.batchSize;

        if (twoPCData.isCollectable()) {
            for (Enriched<T> record : batch) {
                collector.collect(record);
            }
        }

        if (batch.size() >= batchSize) {
            votes.remove(tid);
            counts.remove(tid);
        }
    }
}
