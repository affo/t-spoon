package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.query.QueryTuple;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OptimisticOpenOperator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * Created by affo on 13/07/17.
 */
public class OTStream<T> implements TStream<T> {
    private DataStream<Enriched<T>> ds;

    OTStream(DataStream<Enriched<T>> ds) {
        this.ds = ds;
    }

    public static <T> OTStream<T> fromStream(DataStream<T> ds) {
        // TODO hack: fix lifting type
        TypeInformation<Enriched<T>> type = ds
                .map(new MapFunction<T, Enriched<T>>() {
                    @Override
                    public Enriched<T> map(T t) throws Exception {
                        return null;
                    }
                }).getType();
        DataStream<Enriched<T>> enriched = ds
                .transform("open", type, new OptimisticOpenOperator<>())
                .name("OpenTransaction")
                .setParallelism(1);
        return new OTStream<>(enriched);
    }

    public <U> OTStream<U> map(MapFunction<T, U> fn, int par) {
        return new OTStream<>(
                ds.map(new MapFunction<Enriched<T>, Enriched<U>>() {
                    @Override
                    public Enriched<U> map(Enriched<T> e) throws Exception {
                        return e.replace(fn.map(e.value()));
                    }
                }));
    }

    public <U> OTStream<U> flatMap(it.polimi.affetti.tspoon.common.FlatMapFunction<T, U> flatMapFunction, int par) {
        return new OTStream<>(
                ds.flatMap(
                        new FlatMapFunction<Enriched<T>, Enriched<U>>() {
                            @Override
                            public void flatMap(Enriched<T> e, Collector<Enriched<U>> collector) throws Exception {
                                List<U> out = flatMapFunction.flatMap(e.value());
                                e.tContext().twoPC.batchSize *= out.size();
                                for (U outElement : out) {
                                    collector.collect(e.replace(outElement));
                                }
                            }
                        }));
    }

    public OTStream<T> filter(FilterFunction<T> filterFunction, int par) {
        return new OTStream<>(ds.map(new MapFunction<Enriched<T>, Enriched<T>>() {
            @Override
            public Enriched<T> map(Enriched<T> e) throws Exception {
                T value = filterFunction.filter(e.value()) ? e.value() : null;
                return e.replace(value);
            }
        }));
    }

    public <V> StateStream<T, V> state(
            String nameSpace, OutputTag<Update<V>> updatesTag, KeySelector<T, String> ks,
            StateFunction<T, V> stateFunction, DataStream<QueryTuple> queryStream, int par) {
        ConnectedStreams<Enriched<T>, QueryTuple> connected = ds.connect(queryStream);
        connected = connected.keyBy(
                new KeySelector<Enriched<T>, String>() {
                    @Override
                    public String getKey(Enriched<T> e) throws Exception {
                        return ks.getKey(e.value());
                    }
                },
                new KeySelector<QueryTuple, String>() {
                    @Override
                    public String getKey(QueryTuple queryTuple) throws Exception {
                        return queryTuple.getKey();
                    }
                }
        );

        StateOperator<T, V> stateOperator = new StateOperator<>(stateFunction, updatesTag);
        SingleOutputStreamOperator<Enriched<T>> mainStream = connected.transform(
                "StateOperator: " + nameSpace, ds.getType(), stateOperator)
                .name(nameSpace);

        return new StateStream<>(
                new OTStream<>(mainStream),
                mainStream.getSideOutput(updatesTag));
    }

    @Override
    public DataStream<Enriched<T>> getEnclosingStream() {
        return ds;
    }
}
