package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.tgraph.query.QueryTuple;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 17/07/17.
 */
public interface TStream<T> {
    <U> TStream<U> map(MapFunction<T, U> fn, int par);

    <U> TStream<U> flatMap(FlatMapFunction<T, U> flatMapFunction, int par);

    TStream<T> filter(FilterFunction<T> filterFunction, int par);

    <V> StateStream<T, V> state(
            String nameSpace, OutputTag<Update<V>> updatesTag,
            KeySelector<T, String> ks, StateFunction<T, V> stateFunction,
            DataStream<QueryTuple> queryStream, int par);

    DataStream<Enriched<T>> getEnclosingStream();
}
