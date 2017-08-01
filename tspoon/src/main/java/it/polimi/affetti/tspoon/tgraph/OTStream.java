package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.tgraph.functions.FilterWrapper;
import it.polimi.affetti.tspoon.tgraph.functions.FlatMapWrapper;
import it.polimi.affetti.tspoon.tgraph.functions.MapWrapper;
import it.polimi.affetti.tspoon.tgraph.query.QueryTuple;
import it.polimi.affetti.tspoon.tgraph.state.*;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OptimisticOpenOperator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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

    public static <T> OpenStream<T> fromStream(DataStream<T> ds) {
        // TODO hack: fix lifting type
        TypeInformation<Enriched<T>> type = ds
                .map(new MapFunction<T, Enriched<T>>() {
                    @Override
                    public Enriched<T> map(T t) throws Exception {
                        return null;
                    }
                }).getType();
        OptimisticOpenOperator<T> openOperator = new OptimisticOpenOperator<>();
        SingleOutputStreamOperator<Enriched<T>> enriched = ds
                .transform("open", type, openOperator)
                .name("OpenTransaction")
                .setParallelism(1);

        DataStream<Integer> watermarks = enriched.getSideOutput(openOperator.watermarkTag);
        return new OpenStream<>(new OTStream<>(enriched), watermarks);
    }

    public <U> OTStream<U> map(MapFunction<T, U> fn) {
        return new OTStream<>(ds.map(new MapWrapper<T, U>() {
            @Override
            public U doMap(T e) throws Exception {
                return fn.map(e);
            }
        }));
    }

    public <U> OTStream<U> flatMap(FlatMapFunction<T, U> flatMapFunction) {
        return new OTStream<>(ds.flatMap(
                new FlatMapWrapper<T, U>() {
                    @Override
                    protected List<U> doFlatMap(T value) throws Exception {
                        return flatMapFunction.flatMap(value);
                    }
                }
        ));
    }

    public OTStream<T> filter(FilterFunction<T> filterFunction) {
        return new OTStream<>(ds.map(new FilterWrapper<T>() {
            @Override
            protected boolean doFilter(T value) throws Exception {
                return filterFunction.filter(value);
            }
        }));
    }

    public <V> StateStream<T, V> state(
            String nameSpace, OutputTag<Update<V>> updatesTag, KeySelector<T, String> ks,
            StateFunction<T, V> stateFunction, DataStream<QueryTuple> queryStream, int partitioning) {
        ConnectedStreams<Enriched<T>, QueryTuple> connected = ds.connect(queryStream);
        connected = connected.keyBy(
                new KeySelector<Enriched<T>, String>() {
                    @Override
                    public String getKey(Enriched<T> e) throws Exception {
                        return ks.getKey(e.value);
                    }
                },
                new KeySelector<QueryTuple, String>() {
                    @Override
                    public String getKey(QueryTuple queryTuple) throws Exception {
                        return queryTuple.getKey();
                    }
                }
        );

        StateOperator<T, V> stateOperator = new OptimisticStateOperator<>(stateFunction, updatesTag);
        SingleOutputStreamOperator<Enriched<T>> mainStream = connected.transform(
                "StateOperator: " + nameSpace, ds.getType(), stateOperator)
                .name(nameSpace).setParallelism(partitioning);

        return new StateStream<>(
                new OTStream<>(mainStream),
                mainStream.getSideOutput(updatesTag));
    }

    @Override
    public DataStream<Enriched<T>> getEnclosingStream() {
        return ds;
    }
}
