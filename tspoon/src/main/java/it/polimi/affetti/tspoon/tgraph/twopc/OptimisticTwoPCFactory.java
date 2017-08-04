package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.IsolationLevel;
import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.OTStream;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 17/07/17.
 */
public class OptimisticTwoPCFactory implements TwoPCFactory {
    @Override
    public <T> OpenStream<T> open(DataStream<T> ds) {
        return OTStream.fromStream(ds);
    }

    @Override
    public DataStream<Metadata> onClosingSink(DataStream<Metadata> votesMerged) {
        if (TransactionEnvironment.isolationLevel == IsolationLevel.PL4) {
            return votesMerged.flatMap(new StrictnessEnforcer()).setParallelism(1);
        }

        return votesMerged;
    }
}
