package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.TStream;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

/**
 * Created by affo on 17/07/17.
 */
public interface TwoPCFactory extends Serializable {
    <T> TStream<T> open(DataStream<T> ds);

    void close(DataStream<TwoPCData> voteStream);
}
