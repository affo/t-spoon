package it.polimi.affetti.tspoon.tgraph.query;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by affo on 20/07/17.
 */
// TODO implement
public class QuerySource implements SourceFunction<QueryTuple> {
    private int limit = 10;

    @Override
    public void run(SourceContext<QueryTuple> sourceContext) throws Exception {
        while (limit > 0) {
            sourceContext.collect(new QueryTuple());
            limit--;
        }
    }

    @Override
    public void cancel() {

    }
}
