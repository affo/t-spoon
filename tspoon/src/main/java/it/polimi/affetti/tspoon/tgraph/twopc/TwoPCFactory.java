package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.tgraph.Metadata;
import it.polimi.affetti.tspoon.tgraph.TransactionEnvironment;
import it.polimi.affetti.tspoon.tgraph.query.MultiStateQuery;
import it.polimi.affetti.tspoon.tgraph.query.Query;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

/**
 * Created by affo on 17/07/17.
 */
public interface TwoPCFactory extends Serializable {
    <T> OpenStream<T> open(DataStream<T> ds, DataStream<MultiStateQuery> queryStream, int tGraphId);

    DataStream<Metadata> onClosingSink(DataStream<Metadata> votesMerged, TransactionEnvironment transactionEnvironment);
}
