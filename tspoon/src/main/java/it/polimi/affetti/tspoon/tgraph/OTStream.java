package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.state.OptimisticStateOperator;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.OutputTag;

/**
 * Created by affo on 13/07/17.
 */
public class OTStream<T> extends AbstractTStream<T> {
    public OTStream(DataStream<Enriched<T>> enriched, int tGraphID) {
        super(enriched, tGraphID);
    }

    public static <T> OpenStream<T> fromStream(DataStream<T> ds, int tGraphId) {
        OpenOutputs<T> outputs = AbstractTStream.open(ds, tGraphId);
        return new OpenStream<>(
                new OTStream<>(outputs.enrichedDataStream, tGraphId),
                outputs.watermarks, outputs.tLog);
    }

    @Override
    protected <U> OTStream<U> replace(DataStream<Enriched<U>> newStream) {
        return new OTStream<>(newStream, tGraphID);
    }

    @Override
    protected <V> StateOperator<T, V> getStateOperator(
            String nameSpace, OutputTag<Update<V>> updatesTag, StateFunction<T, V> stateFunction) {
        return new OptimisticStateOperator<>(
                tGraphID, nameSpace, stateFunction, updatesTag,
                getTransactionEnvironment().createTransactionalRuntimeContext()
        );
    }
}
