package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.OTStream;
import it.polimi.affetti.tspoon.tgraph.TStream;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 17/07/17.
 */
public class OptimisticTwoPCFactory implements TwoPCFactory {
    @Override
    public <T> TStream<T> open(DataStream<T> ds) {
        return OTStream.fromStream(ds);
    }

    @Override
    public void close(DataStream<TwoPCData> voteStream) {
        voteStream.addSink(new OptimisticCloseSink());
    }
}
