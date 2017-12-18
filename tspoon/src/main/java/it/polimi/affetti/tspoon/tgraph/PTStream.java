package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.state.PessimisticStateOperator;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import it.polimi.affetti.tspoon.tgraph.twopc.PessimisticOpenOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 13/07/17.
 */
public class PTStream<T> extends AbstractTStream<T> {
    public PTStream(DataStream<Enriched<T>> enriched) {
        super(enriched);
    }

    public static <T> OpenStream<T> fromStream(DataStream<T> ds) {
        TypeInformation<Enriched<T>> type = Enriched.getTypeInfo(ds.getType());
        PessimisticOpenOperator<T> openOperator = new PessimisticOpenOperator<>(
                getTransactionEnvironment().createTransactionalRuntimeContext());
        SingleOutputStreamOperator<Enriched<T>> enriched = ds
                .transform("open", type, openOperator)
                .name("OpenTransaction")
                .setParallelism(1);

        DataStream<Integer> watermarks = enriched.getSideOutput(openOperator.watermarkTag);
        DataStream<Tuple2<Long, Vote>> tLog = enriched.getSideOutput(openOperator.logTag);
        return new OpenStream<>(new PTStream<>(enriched), watermarks, tLog);
    }

    @Override
    protected <U> PTStream<U> replace(DataStream<Enriched<U>> newStream) {
        applySchedulerIfNecessary(newStream);
        return new PTStream<>(newStream);
    }

    @Override
    protected <V> StateOperator<T, V> getStateOperator(
            String nameSpace, OutputTag<Update<V>> updatesTag,
            StateFunction<T, V> stateFunction) {
        PessimisticStateOperator<T, V> stateOperator = new PessimisticStateOperator<>(
                nameSpace, stateFunction, updatesTag,
                getTransactionEnvironment().createTransactionalRuntimeContext());

        if (getTransactionEnvironment().getIsolationLevel() != IsolationLevel.PL4) {
            long deadlockTimeout = getTransactionEnvironment().getDeadlockTimeout();
            stateOperator.enableDeadlockDetection(deadlockTimeout);
        }

        return stateOperator;
    }

    private <U> void applySchedulerIfNecessary(DataStream<Enriched<U>> newStream) {
        boolean isNecessary = getTransactionEnvironment().getIsolationLevel() == IsolationLevel.PL4 &&
                true; // TODO extend condition
        if (isNecessary) {
            applyScheduler(newStream);
        }
    }

    private <U> void applyScheduler(DataStream<Enriched<U>> newStream) {
        // TODO apply the scheduler, please
    }
}
