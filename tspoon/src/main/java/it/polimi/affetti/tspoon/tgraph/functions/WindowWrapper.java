package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by affo on 28/07/17.
 *
 * Requires keyBy timestamp
 */
public abstract class WindowWrapper<I, O> implements FlatMapFunction<Enriched<I>, Enriched<O>> {

    private Map<Integer, List<Enriched<I>>> batches = new HashMap<>();

    private List<Enriched<I>> addElement(int timestamp, Enriched<I> element) {
        int size = element.metadata.batchSize;
        List<Enriched<I>> batch = batches.computeIfAbsent(timestamp, k -> new ArrayList<>(size));
        batch.add(element);
        return batch;
    }

    @Override
    public void flatMap(Enriched<I> element, Collector<Enriched<O>> collector) throws Exception {
        int timestamp = element.metadata.timestamp;
        List<Enriched<I>> batch = addElement(timestamp, element);

        if (batch.size() >= element.metadata.batchSize) {
            batches.remove(timestamp);

            Stream<Metadata> metas = batch.stream().map(el -> el.metadata);
            Metadata metadata = metas.reduce((accumulator, meta) -> {
                accumulator.cohorts.addAll(meta.cohorts);
                accumulator.vote = accumulator.vote.merge(meta.vote);
                accumulator.dependencyTracking.addAll(meta.dependencyTracking);
                return accumulator;
            }).orElseThrow(() -> new RuntimeException("Empty batch in windowing function")); // merge metadata
            metadata.setBatchSize(1);

            List<I> values = batch.stream().map(el -> el.value).collect(Collectors.toList());
            O result = apply(values);
            collector.collect(Enriched.of(metadata, result));
        }
    }

    protected abstract O apply(List<I> value) throws Exception;
}
