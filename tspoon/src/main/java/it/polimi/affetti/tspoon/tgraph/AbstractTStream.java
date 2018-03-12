package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.common.FlatMapFunction;
import it.polimi.affetti.tspoon.common.PartitionOrBcastPartitioner;
import it.polimi.affetti.tspoon.common.TWindowFunction;
import it.polimi.affetti.tspoon.tgraph.functions.*;
import it.polimi.affetti.tspoon.tgraph.query.*;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenOperator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.util.OutputTag;

import java.util.Collections;
import java.util.List;

/**
 * Created by affo on 13/07/17.
 */
public abstract class AbstractTStream<T> implements TStream<T> {
    protected final int tGraphID;
    protected static TransactionEnvironment transactionEnvironment;

    protected DataStream<Enriched<T>> dataStream;
    protected SplitStream<Query> queryStream;

    /**
     *
     * @param enriched
     * @param queryStream can be `null` for testing purposes
     * @param tGraphID
     */
    public AbstractTStream(DataStream<Enriched<T>> enriched, SplitStream<Query> queryStream, int tGraphID) {
        this.dataStream = enriched;
        this.queryStream = queryStream;
        this.tGraphID = tGraphID;
    }

    public static void setTransactionEnvironment(TransactionEnvironment transactionEnvironment) {
        AbstractTStream.transactionEnvironment = transactionEnvironment;
    }

    public static TransactionEnvironment getTransactionEnvironment() {
        return transactionEnvironment;
    }

    protected abstract <U> AbstractTStream<U> replace(DataStream<Enriched<U>> newStream);

    protected abstract <V> StateOperator<T, V> getStateOperator(
            String nameSpace, OutputTag<Update<V>> updatesTag,
            StateFunction<T, V> stateFunction, KeySelector<T, String> ks);

    protected static <T> OpenOutputs<T> open(
            DataStream<T> dataStream, DataStream<MultiStateQuery> inputQueryStream, int tGraphId) {
        TypeInformation<Enriched<T>> type = Enriched.getTypeInfo(dataStream.getType());
        OpenOperator<T> openOperator = new OpenOperator<>(
                transactionEnvironment.createTransactionalRuntimeContext(), tGraphId);
        SingleOutputStreamOperator<Enriched<T>> enriched = dataStream
                .transform("open", type, openOperator)
                .name("OpenTransaction")
                .setParallelism(1);

        DataStream<Integer> watermarks = enriched.getSideOutput(openOperator.watermarkTag);
        DataStream<Tuple2<Long, Vote>> tLog = enriched.getSideOutput(openOperator.logTag);

        SplitStream<Query> queryStream = null;
        if (inputQueryStream != null) {
            queryStream = watermarks.connect(inputQueryStream)
                    .flatMap(new QueryProcessor())
                    .name("AssignWatermark")
                    .split(query -> Collections.singleton(query.getNameSpace()));
        }

        return new OpenOutputs<>(enriched, queryStream, tLog, watermarks);
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

        StateOperator<T, V> stateOperator = getStateOperator(nameSpace, updatesTag, stateFunction, ks);
        // broadcasting queries to every replica
        DataStream<Query> selected = queryStream.select(nameSpace);
        selected = PartitionOrBcastPartitioner.apply(selected);
        ConnectedStreams<Enriched<T>, Query> connected = dataStream.connect(selected);
        SingleOutputStreamOperator<Enriched<T>> mainStream =
                connected.transform(
                        "StateOperator: " + nameSpace, dataStream.getType(), stateOperator)
                        .name(nameSpace).setParallelism(partitioning);

        DataStream<Update<V>> updates = mainStream.getSideOutput(updatesTag);
        DataStream<QueryResult> queryResults = mainStream.getSideOutput(stateOperator.queryResultTag);

        queryResults = queryResults
                .keyBy(queryResult -> queryResult.queryID)
                .flatMap(new QueryResultMerger(transactionEnvironment.getOnQueryResult()))
                .name("QueryResultMerger");

        // TODO should merge every result to rebuild the multiStateQuery...
        return new StateStream<>(replace(mainStream), updates, queryResults);
    }

    @Override
    public DataStream<Enriched<T>> getEnclosingStream() {
        return dataStream;
    }

    protected static class OpenOutputs<T> {
        public DataStream<Enriched<T>> enrichedDataStream;
        public SplitStream<Query> queryStream;
        public DataStream<Tuple2<Long, Vote>> tLog;
        public DataStream<Integer> watermarks;

        public OpenOutputs(
                DataStream<Enriched<T>> enrichedDataStream,
                SplitStream<Query> queryStream,
                DataStream<Tuple2<Long, Vote>> tLog,
                DataStream<Integer> watermarks) {
            this.enrichedDataStream = enrichedDataStream;
            this.queryStream = queryStream;
            this.tLog = tLog;
            this.watermarks = watermarks;
        }
    }
}
