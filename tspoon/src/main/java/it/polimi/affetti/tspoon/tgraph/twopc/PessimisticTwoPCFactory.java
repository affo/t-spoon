package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.PTStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.query.MultiStateQuery;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 17/07/17.
 */
public class PessimisticTwoPCFactory implements TwoPCFactory {
    @Override
    public <T> OpenStream<T> open(DataStream<T> ds, DataStream<MultiStateQuery> queryStream, int tGraphID) {
        return PTStream.fromStream(ds, queryStream, tGraphID);
    }

    @Override
    public DataStream<Metadata> onClosingSink(
            DataStream<Metadata> votesMerged, TransactionEnvironment transactionEnvironment) {
        // does nothing
        return votesMerged;
    }
}
