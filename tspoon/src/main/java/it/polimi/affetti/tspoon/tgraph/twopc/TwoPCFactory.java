package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

/**
 * Created by affo on 17/07/17.
 */
public interface TwoPCFactory extends Serializable {
    <T> OpenStream<T> open(DataStream<T> ds, int tGraphId);

    DataStream<Metadata> onClosingSink(DataStream<Metadata> votesMerged, TransactionEnvironment transactionEnvironment);
}
