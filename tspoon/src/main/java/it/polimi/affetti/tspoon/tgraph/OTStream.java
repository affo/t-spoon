package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.state.OptimisticStateOperator;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import it.polimi.affetti.tspoon.tgraph.twopc.OptimisticOpenOperator;
import it.polimi.affetti.tspoon.tgraph.twopc.TwoPCFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 13/07/17.
 */
public class OTStream<T> extends AbstractTStream<T> {
    public OTStream(DataStream<Enriched<T>> enriched) {
        super(enriched);
    }

    public static <T> OpenStream<T> fromStream(DataStream<T> ds) {
        TypeInformation<Enriched<T>> type = Enriched.getTypeInfo(ds.getType());
        OptimisticOpenOperator<T> openOperator = new OptimisticOpenOperator<>(
                transactionEnvironment.createTransactionalRuntimeContext());
        SingleOutputStreamOperator<Enriched<T>> enriched = ds
                .transform("open", type, openOperator)
                .name("OpenTransaction")
                .setParallelism(1);

        DataStream<Integer> watermarks = enriched.getSideOutput(openOperator.watermarkTag);
        DataStream<Tuple2<Long, Vote>> tLog = enriched.getSideOutput(openOperator.logTag);
        return new OpenStream<>(new OTStream<>(enriched), watermarks, tLog);
    }

    @Override
    protected <U> OTStream<U> replace(DataStream<Enriched<U>> newStream) {
        return new OTStream<>(newStream);
    }

    @Override
    protected <V> StateOperator<T, V> getStateOperator(
            String nameSpace, OutputTag<Update<V>> updatesTag, StateFunction<T, V> stateFunction) {
        return new OptimisticStateOperator<>(
                nameSpace, stateFunction, updatesTag,
                getTransactionEnvironment().createTransactionalRuntimeContext()
        );
    }
}
