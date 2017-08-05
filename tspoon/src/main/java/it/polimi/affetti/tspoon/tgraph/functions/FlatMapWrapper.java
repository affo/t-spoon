package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.tgraph.Enriched;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

/**
 * Created by affo on 28/07/17.
 */
public abstract class FlatMapWrapper<I, O> implements FlatMapFunction<Enriched<I>, Enriched<O>> {
    @Override
    public void flatMap(Enriched<I> e, Collector<Enriched<O>> collector) throws Exception {
        List<O> outputList = doFlatMap(e.value);

        if (outputList == null || outputList.isEmpty()) {
            return;
        }

        // TODO the buffer size can decrease!
        e.metadata.multiplyBatchSize(outputList.size());
        for (O outElement : outputList) {
            collector.collect(e.replace(outElement));
        }
    }

    protected abstract List<O> doFlatMap(I value) throws Exception;
}
