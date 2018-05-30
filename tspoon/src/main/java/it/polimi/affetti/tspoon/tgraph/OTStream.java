package it.polimi.affetti.tspoon.tgraph;

import it.polimi.affetti.tspoon.tgraph.query.MultiStateQuery;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import it.polimi.affetti.tspoon.tgraph.state.OptimisticStateOperator;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateOperator;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;

/**
 * Created by affo on 13/07/17.
 */
public class OTStream<T> extends AbstractTStream<T> {
    public OTStream(DataStream<Enriched<T>> enriched, SplitStream<Query> queryStream, int tGraphID) {
        super(enriched, queryStream, tGraphID);
    }

    public static <T> OpenStream<T> fromStream(
            DataStream<T> ds, DataStream<MultiStateQuery> queryStream, int tGraphId) {
        OpenOutputs<T> outputs = AbstractTStream.open(ds, queryStream, tGraphId);
        return new OpenStream<>(
                new OTStream<>(outputs.enrichedDataStream, outputs.queryStream, tGraphId),
                outputs.watermarks);
    }

    @Override
    protected <U> OTStream<U> replace(DataStream<Enriched<U>> newStream) {
        return new OTStream<>(newStream, queryStream, tGraphID);
    }

    @Override
    protected <V> StateOperator<T, V> getStateOperator(
            String nameSpace, StateFunction<T, V> stateFunction, KeySelector<T, String> ks) {
        return new OptimisticStateOperator<>(
                tGraphID, nameSpace, stateFunction, ks,
                getTransactionEnvironment().createTransactionalRuntimeContext(tGraphID)
        );
    }
}
