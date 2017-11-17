package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.state.PessimisticStateOperator;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import it.polimi.affetti.tspoon.tgraph.twopc.PessimisticOpenOperator;
import it.polimi.affetti.tspoon.tgraph.twopc.TwoPCFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 13/07/17.
 */
public class PTStream<T> extends AbstractTStream<T> {
    private final TwoPCFactory factory;

    PTStream(DataStream<Enriched<T>> ds, TwoPCFactory factory) {
        super(ds);
        this.factory = factory;
    }

    @Override
    protected <U> PTStream<U> replace(DataStream<Enriched<U>> newStream) {
        return new PTStream<>(newStream, factory);
    }

    public static <T> OpenStream<T> fromStream(DataStream<T> ds, TwoPCFactory factory) {
        // TODO hack: fix lifting type
        TypeInformation<Enriched<T>> type = ds
                .map(new MapFunction<T, Enriched<T>>() {
                    @Override
                    public Enriched<T> map(T t) throws Exception {
                        return null;
                    }
                }).getType();
        PessimisticOpenOperator<T> openOperator = new PessimisticOpenOperator<>(factory.getSourceTransactionCloser());
        SingleOutputStreamOperator<Enriched<T>> enriched = ds
                .transform("open", type, openOperator)
                .name("OpenTransaction")
                .setParallelism(1);

        DataStream<Tuple2<Long, Vote>> tLog = enriched.getSideOutput(openOperator.logTag);
        return new OpenStream<>(new PTStream<>(enriched, factory), null, tLog);
    }

    @Override
    protected <V> StateOperator<T, V> getStateOperator(
            String nameSpace, OutputTag<Update<V>> updatesTag,
            StateFunction<T, V> stateFunction) {
        PessimisticStateOperator<T, V> stateOperator = new PessimisticStateOperator<>(
                nameSpace, stateFunction, updatesTag, factory.getAtStateTransactionCloser());

        TransactionEnvironment tEnv = TransactionEnvironment.get();
        if (tEnv.getIsolationLevel() != IsolationLevel.PL4) {
            long deadlockTimeout = tEnv.getDeadlockTimeout();
            stateOperator.enableDeadlockDetection(deadlockTimeout);
        }

        return stateOperator;
    }
}
