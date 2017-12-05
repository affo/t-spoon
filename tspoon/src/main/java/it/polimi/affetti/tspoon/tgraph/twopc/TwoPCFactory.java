package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

/**
 * Created by affo on 17/07/17.
 */
public interface TwoPCFactory extends Serializable {
    <T> OpenStream<T> open(DataStream<T> ds);

    DataStream<Metadata> onClosingSink(DataStream<Metadata> votesMerged);

    <T> TransactionsIndex<T> getTransactionsIndex();
}
