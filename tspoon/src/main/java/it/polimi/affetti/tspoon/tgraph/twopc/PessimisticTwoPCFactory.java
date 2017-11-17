package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.PTStream;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 17/07/17.
 */
public class PessimisticTwoPCFactory extends AbstractTwoPCFactory {
    @Override
    public <T> OpenStream<T> open(DataStream<T> ds) {
        return PTStream.fromStream(ds, this);
    }

    @Override
    public DataStream<Metadata> onClosingSink(DataStream<Metadata> votesMerged) {
        // does nothing
        return votesMerged;
    }
}
