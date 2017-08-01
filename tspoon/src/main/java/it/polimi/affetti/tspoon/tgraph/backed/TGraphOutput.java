package it.polimi.affetti.tspoon.tgraph.backed;

import it.polimi.affetti.tspoon.tgraph.TransactionResult;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 27/07/17.
 */
public class TGraphOutput<O, S> {
    public final DataStream<Integer> watermarks;
    public final DataStream<Update<S>> updates;
    public final DataStream<TransactionResult<O>> output;

    public TGraphOutput(
            DataStream<Integer> watermarks, DataStream<Update<S>> updates, DataStream<TransactionResult<O>> output) {
        this.watermarks = watermarks;
        this.updates = updates;
        this.output = output;
    }
}
