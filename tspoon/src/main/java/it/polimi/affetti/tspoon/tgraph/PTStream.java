package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.functions.Scheduler;
import it.polimi.affetti.tspoon.tgraph.state.*;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 13/07/17.
 */
public class PTStream<T> extends AbstractTStream<T> {
    public PTStream(DataStream<Enriched<T>> enriched) {
        super(enriched);
    }

    public static <T> OpenStream<T> fromStream(DataStream<T> ds) {
        OpenOutputs<T> outputs = AbstractTStream.open(ds);
        return new OpenStream<>(
                new PTStream<>(outputs.enrichedDataStream),
                outputs.watermarks, outputs.tLog);
    }

    @Override
    protected <U> PTStream<U> replace(DataStream<Enriched<U>> newStream) {
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

    @Override
    public <V> StateStream<T, V> state(
            String nameSpace,
            OutputTag<Update<V>> updatesTag,
            KeySelector<T, String> ks,
            StateFunction<T, V> stateFunction,
            int partitioning) {
        dataStream = applySchedulerIfNecessary(dataStream);
        return super.state(nameSpace, updatesTag, ks, stateFunction, partitioning);
    }

    private <U> DataStream<Enriched<U>> applySchedulerIfNecessary(DataStream<Enriched<U>> newStream) {
        IsolationLevel isolationLevel = getTransactionEnvironment().getIsolationLevel();

        if (isolationLevel == IsolationLevel.PL4) {
            return applyScheduler(newStream);
        }

        return newStream;
    }

    private <U> DataStream<Enriched<U>> applyScheduler(DataStream<Enriched<U>> newStream) {
        return newStream
                .flatMap(new Scheduler<>())
                .setParallelism(1)
                .name("Scheduler");
    }
}
