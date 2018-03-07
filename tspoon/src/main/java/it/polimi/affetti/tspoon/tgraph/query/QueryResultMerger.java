package it.polimi.affetti.tspoon.tgraph.query;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Created by affo on 06/03/18.
 */
public class QueryResultMerger implements FlatMapFunction<QueryResult, QueryResult> {
    private final int partitioning;
    private final Map<QueryID, Tuple2<QueryResult, Integer>> partialResults = new HashMap<>();
    private final OnQueryResult onQueryResult;

    public QueryResultMerger(OnQueryResult onQueryResult, int partitioning) {
        this.onQueryResult = onQueryResult;
        this.partitioning = partitioning;
    }

    @Override
    public void flatMap(QueryResult queryResult, Collector<QueryResult> collector) throws Exception {
        Tuple2<QueryResult, Integer> tuple2 = partialResults.computeIfAbsent(queryResult.queryID, queryID ->
                Tuple2.of(new QueryResult(queryID), partitioning));

        tuple2.f1--;
        tuple2.f0.merge(queryResult);

        if (tuple2.f1 == 0) {
            partialResults.remove(queryResult.queryID);
            collector.collect(tuple2.f0);
            onQueryResult.accept(tuple2.f0);
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
