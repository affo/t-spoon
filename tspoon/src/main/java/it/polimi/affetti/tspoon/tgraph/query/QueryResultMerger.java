package it.polimi.affetti.tspoon.tgraph.query;


import it.polimi.affetti.tspoon.metrics.MetricAccumulator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by affo on 06/03/18.
 */
public class QueryResultMerger extends RichFlatMapFunction<QueryResult, QueryResult> {
    public final static String NUMBER_OF_PARTITIONS_ACC = "number-of-partitions-per-query";
    private final Map<QueryID, Tuple2<QueryResult, Integer>> partialResults = new HashMap<>();
    private final OnQueryResult onQueryResult;

    private final MetricAccumulator partitionsPerQuery = new MetricAccumulator();

    public QueryResultMerger(OnQueryResult onQueryResult) {
        this.onQueryResult = onQueryResult;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(NUMBER_OF_PARTITIONS_ACC, partitionsPerQuery);
    }

    @Override
    public void flatMap(QueryResult queryResult, Collector<QueryResult> collector) throws Exception {
        Tuple2<QueryResult, Integer> tuple2 = partialResults.computeIfAbsent(
                queryResult.queryID, queryID -> {
                    partitionsPerQuery.add((double) queryResult.batchSize);
                    return Tuple2.of(new QueryResult(queryID, 1), queryResult.batchSize);
                });

        tuple2.f1--;
        tuple2.f0.merge(queryResult);

        if (tuple2.f1 == 0) {
            partialResults.remove(queryResult.queryID);
            onQueryResult.accept(tuple2.f0);
            collector.collect(tuple2.f0);
        }
    }

    public interface OnQueryResult extends Consumer<QueryResult>, Serializable {
    }

    public static class PrintQueryResult implements OnQueryResult {

        @Override
        public void accept(QueryResult queryResult) {
            System.out.println(">>> " + queryResult.toString());
        }
    }

    public static class NOPOnQueryResult implements OnQueryResult {

        @Override
        public void accept(QueryResult queryResult) {
            // does nothing
        }
    }
}
