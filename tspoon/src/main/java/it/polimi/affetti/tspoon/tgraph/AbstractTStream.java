package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.common.TWindowFunction;
import it.polimi.affetti.tspoon.tgraph.functions.*;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.ConcreteOpenOperator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * Created by affo on 13/07/17.
 */
public abstract class AbstractTStream<T> implements TStream<T> {
    protected static TransactionEnvironment transactionEnvironment;

    protected DataStream<Enriched<T>> dataStream;

    public AbstractTStream(DataStream<Enriched<T>> enriched) {
        this.dataStream = enriched;
    }

    public static void setTransactionEnvironment(TransactionEnvironment transactionEnvironment) {
        AbstractTStream.transactionEnvironment = transactionEnvironment;
    }

    public static TransactionEnvironment getTransactionEnvironment() {
        return transactionEnvironment;
    }

    protected abstract <U> AbstractTStream<U> replace(DataStream<Enriched<U>> newStream);

    protected abstract <V> StateOperator<T, V> getStateOperator(
            String nameSpace, OutputTag<Update<V>> updatesTag, StateFunction<T, V> stateFunction);

    protected static <T> OpenOutputs<T> open(DataStream<T> dataStream) {
        TypeInformation<Enriched<T>> type = Enriched.getTypeInfo(dataStream.getType());
        ConcreteOpenOperator<T> openOperator = new ConcreteOpenOperator<>(
                transactionEnvironment.createTransactionalRuntimeContext());
        SingleOutputStreamOperator<Enriched<T>> enriched = dataStream
                .transform("open", type, openOperator)
                .name("OpenTransaction")
                .setParallelism(1);

        DataStream<Integer> watermarks = enriched.getSideOutput(openOperator.watermarkTag);
        DataStream<Tuple2<Long, Vote>> tLog = enriched.getSideOutput(openOperator.logTag);
        return new OpenOutputs<>(enriched, watermarks, tLog);
    }

    public <U> AbstractTStream<U> map(MapFunction<T, U> fn) {
        return replace(dataStream.map(new MapWrapper<T, U>() {
            @Override
            public U doMap(T e) throws Exception {
                return fn.map(e);
            }
        }));
    }

    public <U> AbstractTStream<U> flatMap(FlatMapFunction<T, U> flatMapFunction) {
        return replace(dataStream.flatMap(
                new FlatMapWrapper<T, U>() {
                    @Override
                    protected List<U> doFlatMap(T value) throws Exception {
                        return flatMapFunction.flatMap(value);
                    }
                }));
    }

    @Override
    public <U> TStream<U> window(TWindowFunction<T, U> windowFunction) {
        DataStream<Enriched<U>> windowed = dataStream.keyBy(
                new KeySelector<Enriched<T>, Integer>() {
                    @Override
                    public Integer getKey(Enriched<T> enriched) throws Exception {
                        return enriched.metadata.timestamp;
                    }
                })
                .flatMap(new WindowWrapper<T, U>() {
                    @Override
                    protected U apply(List<T> value) throws Exception {
                        return windowFunction.apply(value);
                    }
                }).name("TWindow");
        return replace(windowed);
    }

    public AbstractTStream<T> filter(FilterFunction<T> filterFunction) {
        return replace(dataStream.map(new FilterWrapper<T>() {
            @Override
            protected boolean doFilter(T value) throws Exception {
                return filterFunction.filter(value);
            }
        }));
    }

    @Override
    public TStream<T> keyBy(KeySelector<T, ?> keySelector) {
        dataStream = dataStream.keyBy(new KeySelectorWrapper<T>() {
            @Override
            protected Object doGetKey(T value) throws Exception {
                return keySelector.getKey(value);
            }
        });

        return this;
    }

    public <V> StateStream<T, V> state(
            String nameSpace, OutputTag<Update<V>> updatesTag, KeySelector<T, String> ks,
            StateFunction<T, V> stateFunction, int partitioning) {
        keyBy(ks);

        StateOperator<T, V> stateOperator = getStateOperator(nameSpace, updatesTag, stateFunction);
        SingleOutputStreamOperator<Enriched<T>> mainStream = dataStream.transform(
                "StateOperator: " + nameSpace, dataStream.getType(), stateOperator)
                .name(nameSpace).setParallelism(partitioning);

        DataStream<Update<V>> updates = mainStream.getSideOutput(updatesTag);
        return new StateStream<>(replace(mainStream), updates);
    }

    @Override
    public DataStream<Enriched<T>> getEnclosingStream() {
        return dataStream;
    }

    protected static class OpenOutputs<T> {
        public DataStream<Enriched<T>> enrichedDataStream;
        public DataStream<Integer> watermarks;
        public DataStream<Tuple2<Long, Vote>> tLog;

        public OpenOutputs(
                DataStream<Enriched<T>> enrichedDataStream,
                DataStream<Integer> watermarks,
                DataStream<Tuple2<Long, Vote>> tLog) {
            this.enrichedDataStream = enrichedDataStream;
            this.watermarks = watermarks;
            this.tLog = tLog;
        }
    }
}
