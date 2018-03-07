package it.polimi.affetti.tspoon.tgraph.state;

import it.polimi.affetti.tspoon.tgraph.TStream;
import it.polimi.affetti.tspoon.tgraph.query.QueryResult;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Created by affo on 20/07/17.
 */
public class StateStream<T, V> {
    public final TStream<T> leftUnchanged;
    public final DataStream<Update<V>> updates;
    public final DataStream<QueryResult> queryResults;

    public StateStream(TStream<T> leftUnchanged,
                       DataStream<Update<V>> updates,
                       DataStream<QueryResult> queryResults) {
        this.leftUnchanged = leftUnchanged;
        this.updates = updates;
        this.queryResults = queryResults;
    }
}
