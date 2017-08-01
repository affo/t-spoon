package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.TStream;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 20/07/17.
 */
public class OpenStream<T> {
    public final TStream<T> opened;
    public final DataStream<Integer> watermarks;

    public OpenStream(TStream<T> opened, DataStream<Integer> watermarks) {
        this.opened = opened;
        this.watermarks = watermarks;
    }
}
