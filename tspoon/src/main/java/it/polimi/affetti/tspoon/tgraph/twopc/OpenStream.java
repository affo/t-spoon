package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 20/07/17.
 */
public class OpenStream<T> {
    public final TStream<T> opened;
    public final DataStream<Integer> watermarks;
    public final DataStream<Tuple2<Long, Vote>> wal;

    public OpenStream(TStream<T> opened, DataStream<Integer> watermarks, DataStream<Tuple2<Long, Vote>> wal) {
        this.opened = opened;
        this.watermarks = watermarks;
        this.wal = wal;
    }
}
