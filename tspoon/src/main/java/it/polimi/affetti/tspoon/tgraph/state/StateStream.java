package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 20/07/17.
 */
public class StateStream<T> {
    public final TStream<T> leftUnchanged;
    public final DataStream<QueryResult> queryResults;

    public StateStream(TStream<T> leftUnchanged,
                       DataStream<QueryResult> queryResults) {
        this.leftUnchanged = leftUnchanged;
        this.queryResults = queryResults;
    }
}
