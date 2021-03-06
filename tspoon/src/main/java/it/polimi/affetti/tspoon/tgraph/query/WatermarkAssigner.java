package it.polimi.affetti.tspoon.tgraph.query;

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Created by affo on 02/08/17.
 * <p>
 * NOTE the stream of queries is split into different streams by nameSpace
 */
public class WatermarkAssigner implements CoFlatMapFunction<Long, MultiStateQuery, Query> {
    private long currentWatermark;

    @Override
    public void flatMap1(Long watermark, Collector<Query> collector) throws Exception {
        currentWatermark = Math.max(currentWatermark, watermark);
    }

    @Override
    public void flatMap2(MultiStateQuery multiStateQuery, Collector<Query> collector) throws Exception {
        multiStateQuery.setWatermark(currentWatermark);
        for (Query query : multiStateQuery) {
            collector.collect(query);
        }
    }
}
