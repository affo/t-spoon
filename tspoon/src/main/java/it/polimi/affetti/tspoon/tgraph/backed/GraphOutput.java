package it.polimi.affetti.tspoon.tgraph.backed;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 28/07/17.
 */
public class GraphOutput<O> {
    public final DataStream<O> output;

    public GraphOutput(DataStream<O> output) {
        this.output = output;
    }
}
