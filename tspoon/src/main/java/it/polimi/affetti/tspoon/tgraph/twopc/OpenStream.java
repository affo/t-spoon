package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 20/07/17.
 */
public class OpenStream<T> {
    public final TStream<T> opened;
    public final DataStream<Long> watermarks;

    public OpenStream(
            TStream<T> opened,
            DataStream<Long> watermarks) {
        this.opened = opened;
        this.watermarks = watermarks;
    }
}
